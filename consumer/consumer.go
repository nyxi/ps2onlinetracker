package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/nyxi/ps2onlinetracker/ps2api"
	"strconv"
	"strings"
	"time"
)

type Consumer struct {
	RDB        *sql.DB
	EventQueue chan []byte
	API        *ps2api.PS2API
}

func New(dbconnstring string, eventQueue chan []byte, serviceurl string) (*Consumer, error) {
	// Relational database connection setup
	db, err := sql.Open("postgres", dbconnstring)
	if err != nil {
		return nil, err
	}

	// PS2API
	p := ps2api.New(serviceurl)

	return &Consumer{
		API:        p,
		RDB:        db,
		EventQueue: eventQueue,
	}, nil
}

type PlayerLogEvent struct {
	CharacterID int    `json:"character_id"`
	EventName   string `json:"event_name"`
	Timestamp   string `json:"timestamp"`
	WorldID     int    `json:"world_id"`
}

func (pe *PlayerLogEvent) UnmarshalJSON(data []byte) error {
	type Alias PlayerLogEvent
	aux := &struct {
		CharacterID string `json:"character_id"`
		WorldID     string `json:"world_id"`
		*Alias
	}{
		Alias: (*Alias)(pe),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error

	pe.CharacterID, err = strconv.Atoi(aux.CharacterID)
	if err != nil {
		fmt.Println("CharacterID")
		return err
	}
	pe.WorldID, err = strconv.Atoi(aux.WorldID)
	if err != nil {
		fmt.Println("WorldID")
		return err
	}

	return nil
}

type PlayerLogEventRaw struct {
	Payload PlayerLogEvent `json:"payload"`
}

func (c *Consumer) ConsumeLogins() error {
	var rawevent []byte
	var playerevent PlayerLogEventRaw
	var character_ids []int
	var rows *sql.Rows
	var cid int
	var i int
	var characters []ps2api.Character

	batchSize := 40

	// Nuke the rows in online_players so the tracking can start anew
	_, err := c.RDB.Exec("DELETE FROM online_players")
	if err != nil {
		return err
	}

	// Shit load of prepared statements
	// Retrieves a bunch of characters
	character_select_prepared, err := c.RDB.Prepare("SELECT id FROM character_names WHERE id = ANY($1)")
	if err != nil {
		return err
	}
	// Retrieves cached character-to-outfit mappings
	co_select_prepared, err := c.RDB.Prepare("SELECT character_id FROM character_outfit WHERE character_id = ANY($1) AND updated > (now() - interval '2 days')")
	if err != nil {
		return err
	}
	// Add new characters to the database, mostly to get that juicy id to name mapping
	character_insert_prepared, err := c.RDB.Prepare("INSERT INTO character_names (id,name,faction_id,world_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	// Add new outfit to the database, mostly to get that juicy id to alias/name mapping
	outfit_insert_prepared, err := c.RDB.Prepare("INSERT INTO outfit_names (id,alias,name,faction_id,world_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	// Add/update character to outfit mapping for a character
	co_insert_prepared, err := c.RDB.Prepare("INSERT INTO character_outfit (character_id,outfit_id,updated) VALUES ($1, $2, $3) ON CONFLICT (character_id) DO UPDATE SET outfit_id = EXCLUDED.outfit_id, updated = EXCLUDED.updated")
	if err != nil {
		return err
	}
	// Add a character id to the table of currently online players
	online_add_prepared, err := c.RDB.Prepare("INSERT INTO online_players (character_id) VALUES ($1) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	// Remove a character id from the table of currently online players
	online_del_prepared, err := c.RDB.Prepare("DELETE FROM online_players WHERE character_id = $1")
	if err != nil {
		return err
	}

	for {
		// Get and parse event
		rawevent = <-c.EventQueue
		err = json.Unmarshal(rawevent, &playerevent)
		if err != nil {
			fmt.Printf("Consumer hit error %s while unmarshaling event: %s\n", err, rawevent)
			continue
		}

		// Filter events that are not login/logout
		if playerevent.Payload.CharacterID == 0 {
			continue
		}

		// Track online players - does not depend on API calls
		// so can be executed immediately without batching
		if playerevent.Payload.EventName == "PlayerLogin" {
			_, err = online_add_prepared.Exec(playerevent.Payload.CharacterID)
			if err != nil {
				return err
			}
		}
		if playerevent.Payload.EventName == "PlayerLogout" {
			_, err = online_del_prepared.Exec(playerevent.Payload.CharacterID)
			if err != nil {
				return err
			}
		}

		// Keep character_ids free from duplicates
		duplicate := false
		for i = 0; i < len(character_ids); i++ {
			if character_ids[i] == playerevent.Payload.CharacterID {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}

		// Add character id to the list
		character_ids = append(character_ids, playerevent.Payload.CharacterID)
		//fmt.Printf("Consumer: Characters queued for API call: %d\n", len(character_ids))
		// Do nothing unless we have reached the target batch size
		if len(character_ids) < batchSize {
			continue
		}

		// Get the character ids from the database if they exist
		rows, err = character_select_prepared.Query(pq.Array(character_ids))
		if err != nil {
			return err
		}

		// Keep track of what character ids we already have a name for
		// in the database
		var character_db_names []int
		for rows.Next() {
			err = rows.Scan(&cid)
			if err != nil {
				return err
			}
			character_db_names = append(character_db_names, cid)
		}
		// Catch database error
		if err = rows.Err(); err != nil {
			return err
		}
		rows.Close()

		// Get character-outfit mapping from the database
		rows, err = co_select_prepared.Query(pq.Array(character_ids))
		if err != nil {
			return err
		}
		defer rows.Close()

		// Keep track of what character ids we already have an outfit for
		var co_mapping []int
		for rows.Next() {
			err = rows.Scan(&cid)
			if err != nil {
				return err
			}
			co_mapping = append(co_mapping, cid)
		}
		// Catch database error
		if err = rows.Err(); err != nil {
			return err
		}
		rows.Close()

		// Figure out what cids we actually need to call the API for
		var remove_cids []int
		var x int
		for i = 0; i < len(character_ids); i++ {
			first_match := false
			second_match := false
			for x = 0; x < len(character_db_names); x++ {
				if character_ids[i] == character_db_names[x] {
					first_match = true
					break
				}
			}
			for x = 0; x < len(co_mapping); x++ {
				if character_ids[i] == co_mapping[x] {
					second_match = true
					break
				}
			}
			if first_match && second_match {
				remove_cids = append(remove_cids, character_ids[i])
			}
		}

		// Remove the ids we don't need to call the API for from character_ids
		for i = 0; i < len(remove_cids); i++ {
			for index := 0; index < len(character_ids); index++ {
				if remove_cids[i] == character_ids[index] {
					character_ids = append(character_ids[:index], character_ids[index+1:]...)
					break
				}
			}
		}

		// Do nothing if we dont have batchSize ids
		if len(character_ids) < batchSize {
			continue
		}

		// API Call!
		fmt.Println("API Call")
		characters, err = c.API.ResolveCharacterID(character_ids...)
		if err != nil {
			if strings.HasSuffix(err.Error(), "connection reset by peer") {
				fmt.Println("ERROR: API call received connection reset by peer, dumping character_ids and continuing..")
				character_ids = []int{}
				continue
			}
			fmt.Println("ERROR: API call - ", err)
			return err
		}

		// DB Table character_names
		for i = 0; i < len(characters); i++ {
			_, err = character_insert_prepared.Exec(characters[i].CharacterID, characters[i].Name, characters[i].FactionID, characters[i].WorldID)
			if err != nil {
				return err
			}
		}

		// DB Table outfit_names
		for i = 0; i < len(characters); i++ {
			_, err = outfit_insert_prepared.Exec(characters[i].Outfit.OutfitID, characters[i].Outfit.Alias, characters[i].Outfit.Name, characters[i].FactionID, characters[i].WorldID)
			if err != nil {
				return err
			}
		}

		// DB Table character_outfit
		t := time.Now()
		for i = 0; i < len(characters); i++ {
			_, err = co_insert_prepared.Exec(characters[i].CharacterID, characters[i].Outfit.OutfitID, t)
			if err != nil {
				return err
			}
		}

		// Reset character_ids
		character_ids = []int{}
	}
	return nil
}
