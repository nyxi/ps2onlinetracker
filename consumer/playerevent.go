package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

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
		return errors.New(fmt.Sprintf("CharacterID, %s", err))
	}
	pe.WorldID, err = strconv.Atoi(aux.WorldID)
	if err != nil {
		return errors.New(fmt.Sprintf("WorldID, %s", err))
	}

	return nil
}

type PlayerLogEventRaw struct {
	Payload PlayerLogEvent `json:"payload"`
}
