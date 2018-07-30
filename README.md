# PS2OnlineTracker
This application consumes the [Planetside 2 Event Streaming API](https://census.daybreakgames.com/) to track the number of online players per outfit, server and faction.

To achieve this without unduly hammering the official API we cache the information about which outfit a character belongs to and data mapping character and outfit ids to their respective name.

Finally we expose the relevant metrics for ingestion by a [Prometheus](https://prometheus.io/) server.

This application provides the data for [ps2.li](https://ps2.li/)

## Running
At the very least you need a PostgreSQL database (9.5+) and a registered service id for the Census API.

`ps2onlinetracker` is exclusively configured with environment variables;

|Variable|Example value|Notes|
|--------|-------------|-----|
|DB_CONN|postgres://user:password@localhost/dbname?sslmode=disable|SQL connection string|
|METRICS_LISTEN|localhost:8080|Listen address for the metrics webserver|
|SERVICE_ID|myid|Your service id to access Census API|

Setup tables and enter some static data with the included [tables.sql](tables.sql) before running `ps2onlinetracker`.

To run with systemd, see the [included service file](ps2onlinetracker.service).

## Building
You need Golang, I have only tested builds with v1.10.

```
go get ./...
go build ./cmd/ps2onlinetracker/
```

## License
[GPLv3](LICENSE)
