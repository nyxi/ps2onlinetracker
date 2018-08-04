package consumer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/nyxi/ps2onlinetracker/ps2api"
	"github.com/nyxi/ps2onlinetracker/util"
	"strings"
	"time"
)

type Consumer struct {
	Errors     chan error
	RDB        *sql.DB
	EventQueue chan []byte
	API        *ps2api.PS2API
	Statements map[string]*sql.Stmt
}

func New(dbconnstring string, eventQueue chan []byte, serviceurl string) (*Consumer, error) {
	// Relational database connection setup
	db, err := sql.Open("postgres", dbconnstring)
	if err != nil {
		return nil, err
	}

	// PS2API
	p := ps2api.New(serviceurl)

	c := &Consumer{
		Errors:     make(chan error),
		API:        p,
		RDB:        db,
		EventQueue: eventQueue,
	}

	err = c.PrepareStatements()
	return c, err
}

func (c *Consumer) PrepareStatements() error {
	var err error
	m := make(map[string]*sql.Stmt)

	// Prepare statements for adding/removing online players
	m["online_add"], err = c.RDB.Prepare("INSERT INTO online_players (character_id) VALUES ($1) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	m["online_del"], err = c.RDB.Prepare("DELETE FROM online_players WHERE character_id = $1")
	if err != nil {
		return err
	}
	// Retrieves a cached character-to-outfit mapping
	m["co_select"], err = c.RDB.Prepare("SELECT character_id FROM character_outfit WHERE character_id = $1 AND updated > (now() - interval '2 days')")
	if err != nil {
		return err
	}
	// Add new characters to the database, mostly to get that juicy id to name mapping
	m["character_insert"], err = c.RDB.Prepare("INSERT INTO character_names (id,name,faction_id,world_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	// Add new outfit to the database, mostly to get that juicy id to alias/name mapping
	m["outfit_insert"], err = c.RDB.Prepare("INSERT INTO outfit_names (id,alias,name,faction_id,world_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	// Add/update character to outfit mapping for a character
	m["co_insert"], err = c.RDB.Prepare("INSERT INTO character_outfit (character_id,outfit_id,updated) VALUES ($1, $2, $3) ON CONFLICT (character_id) DO UPDATE SET outfit_id = EXCLUDED.outfit_id, updated = EXCLUDED.updated")
	if err != nil {
		return err
	}

	c.Statements = m
	return nil
}

func (c *Consumer) TrackOnline(events chan PlayerLogEvent) {
	// Nuke the rows in online_players so the tracking can start anew
	_, err := c.RDB.Exec("DELETE FROM online_players")
	if err != nil {
		c.Errors <- err
		return
	}

	var playerevent PlayerLogEvent

	for {
		playerevent = <-events
		if playerevent.EventName == "PlayerLogin" {
			_, err = c.Statements["online_add"].Exec(playerevent.CharacterID)
			if err != nil {
				c.Errors <- err
				return
			}
		}
		if playerevent.EventName == "PlayerLogout" {
			_, err = c.Statements["online_del"].Exec(playerevent.CharacterID)
			if err != nil {
				c.Errors <- err
				return
			}
		}
	}
}

func (c *Consumer) IsCharacterIDInStatement(characterID int, statement *sql.Stmt) (bool, error) {
	rows, err := statement.Query(characterID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var dbCharacterID int
	for rows.Next() {
		err = rows.Scan(&dbCharacterID)
		if err != nil {
			return false, err
		}
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	rows.Close()

	if characterID == dbCharacterID {
		return true, nil
	} else {
		return false, nil
	}
}

func (c *Consumer) WriteCharactersToDatabase(characters []ps2api.Character) error {
	var err error
	t := time.Now()
	for i := 0; i < len(characters); i++ {
		// DB Table character_names
		_, err = c.Statements["character_insert"].Exec(characters[i].CharacterID, characters[i].Name, characters[i].FactionID, characters[i].WorldID)
		if err != nil {
			return err
		}
		// DB Table outfit_names
		_, err = c.Statements["outfit_insert"].Exec(characters[i].Outfit.OutfitID, characters[i].Outfit.Alias, characters[i].Outfit.Name, characters[i].FactionID, characters[i].WorldID)
		if err != nil {
			return err
		}
		// DB Table character_outfit
		_, err = c.Statements["co_insert"].Exec(characters[i].CharacterID, characters[i].Outfit.OutfitID, t)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) ResolveCharacterOutfit(cidsChan chan int) {
	var batchedCharacters []int
	var characterID int
	var characters []ps2api.Character
	var coMappingExists bool
	var err error

	batchSize := 40
	for {
		characterID = <-cidsChan

		// Duplicate check
		if util.IsIntInList(characterID, batchedCharacters) {
			continue
		}

		// Skip if we have a cached outfit id mapped for this character id
		coMappingExists, err = c.IsCharacterIDInStatement(characterID, c.Statements["co_select"])
		if err != nil {
			c.Errors <- err
			return
		}
		if coMappingExists {
			continue
		}

		batchedCharacters = append(batchedCharacters, characterID)

		if len(batchedCharacters) < batchSize {
			continue
		}

		characters, err = c.API.ResolveCharacterID(batchedCharacters...)
		if err != nil {
			if strings.HasSuffix(err.Error(), "connection reset by peer") {
				fmt.Println("ERROR: API call received connection reset by peer, dumping current batch and continuing..")
				batchedCharacters = []int{}
				continue
			}
			c.Errors <- err
			return
		}
		fmt.Printf("API call for %d cids yielded %d characters\n", len(batchedCharacters), len(characters))

		err = c.WriteCharactersToDatabase(characters)
		if err != nil {
			c.Errors <- err
			return
		}

		// Reset
		batchedCharacters = []int{}
	}

}

func (c *Consumer) Consume() error {
	var err error
	var rawevent []byte
	var playerevent PlayerLogEventRaw

	onlineTrackerCh := make(chan PlayerLogEvent, 100)
	go c.TrackOnline(onlineTrackerCh)
	resolveCharsOCh := make(chan int, 100)
	go c.ResolveCharacterOutfit(resolveCharsOCh)

	go func() {
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

			onlineTrackerCh <- playerevent.Payload
			resolveCharsOCh <- playerevent.Payload.CharacterID
		}
	}()

	err = <-c.Errors
	return err
}
