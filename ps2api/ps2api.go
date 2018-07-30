package ps2api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Character struct {
	CharacterID int    `json:"character_id"`
	FactionID   int    `json:"faction_id"`
	Name        string `json:"name"`
	Outfit      Outfit `json:"outfit"`
	WorldID     int    `json:"world_id"`
}

func (c *Character) UnmarshalJSON(data []byte) error {
	type Alias Character
	aux := &struct {
		CharacterID string `json:"character_id"`
		FactionID   string `json:"faction_id"`
		Name        struct {
			First string `json:"first"`
		} `json:"name"`
		WorldID string `json:"world_id"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error

	c.CharacterID, err = strconv.Atoi(aux.CharacterID)
	if err != nil {
		fmt.Println("CharacterID")
		return err
	}
	c.FactionID, err = strconv.Atoi(aux.FactionID)
	if err != nil {
		fmt.Println("FactionID")
		return err
	}
	if aux.WorldID == "" {
		fmt.Printf("Skipping malformed character record missing world_id: %s\n", data)
		return nil
	}
	c.WorldID, err = strconv.Atoi(aux.WorldID)
	if err != nil {
		fmt.Println("WorldID")
		return err
	}

	c.Name = aux.Name.First
	return nil
}

type CharacterResponse struct {
	CharacterList []Character `json:"character_list"`
	Returned      int         `json:"returned"`
}

type Outfit struct {
	Alias             string    `json:"alias"`
	AliasLower        string    `json:"alias_lower"`
	LeaderCharacterID int       `json:"leader_character_id"`
	MemberCount       int       `json:"member_count"`
	MemberSinceDate   string    `json:"member_since_date"`
	Name              string    `json:"name"`
	NameLower         string    `json:"name_lower"`
	OutfitID          int       `json:"outfit_id"`
	OutfitIDMerged    int       `json:"outfit_id_merged"`
	TimeCreated       int64     `json:"time_created"`
	TimeCreatedDate   time.Time `json:"time_created_date"`
}

func (o *Outfit) UnmarshalJSON(data []byte) error {
	type Alias Outfit
	aux := &struct {
		LeaderCharacterID string `json:"leader_character_id"`
		MemberCount       string `json:"member_count"`
		OutfitID          string `json:"outfit_id"`
		OutfitIDMerged    string `json:"outfit_id_merged"`
		TimeCreated       string `json:"time_created"`
		TimeCreatedDate   string `json:"time_created_date"`
		*Alias
	}{
		Alias: (*Alias)(o),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var err error

	if aux.LeaderCharacterID == "" {
		fmt.Printf("Skipping malformed outfit record: %s\n", data)
		return nil
	}
	o.LeaderCharacterID, err = strconv.Atoi(aux.LeaderCharacterID)
	if err != nil {
		fmt.Println("LeaderCharacterID")
		return err
	}

	o.MemberCount, err = strconv.Atoi(aux.MemberCount)
	if err != nil {
		fmt.Println("MemberCount")
		return err
	}

	o.OutfitID, err = strconv.Atoi(aux.OutfitID)
	if err != nil {
		fmt.Println("OutfitID")
		return err
	}

	o.OutfitIDMerged, err = strconv.Atoi(aux.OutfitIDMerged)
	if err != nil {
		fmt.Println("OutfitIDMerged")
		return err
	}

	o.TimeCreated, err = strconv.ParseInt(aux.TimeCreated, 10, 64)
	if err != nil {
		fmt.Println("TimeCreated")
		return err
	}

	o.TimeCreatedDate = time.Unix(o.TimeCreated, 0)
	return nil
}

type PS2API struct {
	httpclient *http.Client
	ServiceURL string
}

func New(serviceurl string) *PS2API {
	return &PS2API{
		httpclient: &http.Client{},
		ServiceURL: serviceurl,
	}
}

func (p *PS2API) Get(apipath string, args ...[2]string) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/%s", p.ServiceURL, apipath), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")

	if len(args) > 0 {
		q := req.URL.Query()
		for i := 0; i < len(args); i++ {
			q.Add(args[i][0], args[i][1])
		}
		req.URL.RawQuery = q.Encode()
	}

	resp, err := p.httpclient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprintf("HTTP Status Code: %d", resp.StatusCode))
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func (p *PS2API) ResolveCharacterID(cids ...int) ([]Character, error) {
	var cidsS []string
	for i := 0; i < len(cids); i++ {
		cidsS = append(cidsS, fmt.Sprintf("%d", cids[i]))
	}
	character_ids := [2]string{"character_id", strings.Join(cidsS, ",")}
	resolve := [2]string{"c:resolve", "outfit,world"}
	hide := [2]string{"c:hide", "battle_rank,certs,daily_ribbon,head_id,name.first_lower,times,prestige_level,profile_id,title_id"}

	r, err := p.Get("character", resolve, hide, character_ids)
	if err != nil {
		return nil, err
	}

	var cresp CharacterResponse
	err = json.Unmarshal(r, &cresp)
	if err != nil {
		return nil, err
	}

	return cresp.CharacterList, nil
}
