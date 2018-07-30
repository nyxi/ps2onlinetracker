package censusstreamer

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"time"
)

type SubscribeAction struct {
	Service    string   `json:"service"`
	Action     string   `json:"action"`
	Characters []string `json:"characters"`
	Worlds     []string `json:"worlds"`
	EventNames []string `json:"eventNames"`
}

type Streamer struct {
	Queue        chan []byte
	Subscribe    *SubscribeAction
	WebsocketURL string
}

func NewStreamer(queue chan []byte, subscribe SubscribeAction, websocketurl string) (*Streamer, error) {
	dialer := new(websocket.Dialer)
	_, _, err := dialer.Dial(websocketurl, nil)
	if err != nil {
		return nil, err
	}

	return &Streamer{
		Queue:        queue,
		Subscribe:    &subscribe,
		WebsocketURL: websocketurl,
	}, nil
}

func (s *Streamer) ConnectAndSubscribe() (*websocket.Conn, error) {
	dialer := new(websocket.Dialer)
	conn, _, err := dialer.Dial(s.WebsocketURL, nil)
	if err != nil {
		return nil, err
	}

	w, err := conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return nil, err
	}

	sa, err := json.Marshal(s.Subscribe)
	if err != nil {
		return nil, err
	}

	_, err = w.Write(sa)
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *Streamer) Listen() {
	for {
		conn, err := s.ConnectAndSubscribe()
		if err != nil {
			fmt.Printf("Streamer connect error %s\n", err)
			time.Sleep(15 * time.Second)
			continue
		}

		var r []byte
		for {
			_, r, err = conn.ReadMessage()
			if err != nil {
				fmt.Println(err)
				break
			}
			s.Queue <- r
		}
		fmt.Printf("Streamer read error %s\n", err)
		time.Sleep(15 * time.Second)
	}
}
