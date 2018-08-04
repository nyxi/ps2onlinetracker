package main

import (
	"github.com/nyxi/ps2onlinetracker/censusstreamer"
	"github.com/nyxi/ps2onlinetracker/consumer"
	"github.com/nyxi/ps2onlinetracker/metrics"
	"log"
	"os"
)

func main() {
	// Get configuration from environment variables
	dbconnstr := os.Getenv("DB_CONN")
	if dbconnstr == "" {
		log.Fatalln("Environment variable DB_CONN must be set")
	}
	serviceID := os.Getenv("SERVICE_ID")
	if serviceID == "" {
		log.Fatalln("Environment variable SERVICE_ID must be set")
	}
	metricsListen := os.Getenv("METRICS_LISTEN")
	if metricsListen == "" {
		log.Fatalln("Environment variable METRICS_LISTEN must be set")
	}

	// Get the consumer of events up and running
	eventsqueue := make(chan []byte)
	consum, err := consumer.New(dbconnstr, eventsqueue, "http://census.daybreakgames.com/s:"+serviceID+"/get/ps2")
	if err != nil {
		log.Fatalln(err)
	}
	go func() {
		err := consum.Consume()
		if err != nil {
			log.Fatalln("Consumer error: ", err)
		}
	}()

	// Configure what events to listen to from the Planetside events stream
	subaction := &censusstreamer.SubscribeAction{
		Service:    "event",
		Action:     "subscribe",
		Characters: []string{"all"},
		Worlds:     []string{},
		EventNames: []string{"PlayerLogin", "PlayerLogout"},
	}

	// Get the streamer of events up and running
	streamer, err := censusstreamer.NewStreamer(eventsqueue, *subaction, "wss://push.planetside2.com/streaming?environment=ps2&service-id=s:"+serviceID)
	if err != nil {
		log.Fatalln(err)
	}
	go streamer.Listen()

	m, err := metrics.New(dbconnstr, metricsListen)
	if err != nil {
		log.Fatalln(err)
	}

	m.Webserver.ListenAndServe()
}
