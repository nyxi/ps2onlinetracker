package metrics

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"net/http"
)

type Metrics struct {
	RDB                 *sql.DB
	PreparedStatementOO *sql.Stmt
	PreparedStatementOP *sql.Stmt
	Webserver           *http.Server
}

func New(dbconnstring string, listenstring string) (*Metrics, error) {
	// Relational database connection setup
	db, err := sql.Open("postgres", dbconnstring)
	if err != nil {
		return nil, err
	}

	oo_prepq := "SELECT COUNT(online_players.character_id), outfit_names.alias, outfit_names.name, faction_names.tag, world_names.name FROM online_players INNER JOIN character_outfit ON online_players.character_id=character_outfit.character_id INNER JOIN outfit_names ON character_outfit.outfit_id=outfit_names.id INNER JOIN faction_names ON outfit_names.faction_id=faction_names.id INNER JOIN world_names ON outfit_names.world_id=world_names.id WHERE outfit_id!=0 GROUP BY outfit_names.name,outfit_names.alias,faction_names.tag,world_names.name"
	prepared, err := db.Prepare(oo_prepq)
	if err != nil {
		return nil, err
	}

	op_prepq := "SELECT COUNT(online_players.character_id), faction_names.tag, world_names.name FROM online_players INNER JOIN character_names ON online_players.character_id=character_names.id INNER JOIN faction_names ON character_names.faction_id=faction_names.id INNER JOIN world_names ON character_names.world_id=world_names.id GROUP BY faction_names.tag,world_names.name"
	op_prepared, err := db.Prepare(op_prepq)
	if err != nil {
		return nil, err
	}

	m := &Metrics{
		RDB:                 db,
		PreparedStatementOO: prepared,
		PreparedStatementOP: op_prepared,
		Webserver: &http.Server{
			Addr: listenstring,
		},
	}
	http.HandleFunc("/metrics", m.MetricsHandleF)

	return m, nil
}

func (m *Metrics) GetOutfitOnline() (string, error) {
	rows, err := m.PreparedStatementOO.Query()
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var buffer bytes.Buffer
	buffer.WriteString("# HELP outfit_members_online Number of members currently online from an outfit.\n")
	buffer.WriteString("# TYPE outfit_members_online gauge\n")

	for rows.Next() {
		var (
			count   int
			alias   string
			name    string
			faction string
			server  string
		)
		if err := rows.Scan(&count, &alias, &name, &faction, &server); err != nil {
			return "", err
		}
		buffer.WriteString(fmt.Sprintf("outfit_members_online{alias=\"%s\",name=\"%s\",faction=\"%s\",server=\"%s\"} %d\n", alias, name, faction, server, count))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func (m *Metrics) GetOnlinePlayers() (string, error) {
	rows, err := m.PreparedStatementOP.Query()
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var buffer bytes.Buffer
	buffer.WriteString("# HELP online_players Number of players online.\n")
	buffer.WriteString("# TYPE online_players gauge\n")

	for rows.Next() {
		var (
			count   int
			faction string
			server  string
		)
		if err := rows.Scan(&count, &faction, &server); err != nil {
			return "", err
		}
		buffer.WriteString(fmt.Sprintf("online_players{faction=\"%s\",server=\"%s\"} %d\n", faction, server, count))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func (m *Metrics) MetricsHandleF(w http.ResponseWriter, r *http.Request) {
	bodystring1, err := m.GetOutfitOnline()
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		fmt.Fprint(w, err)
		return
	}
	bodystring2, err := m.GetOnlinePlayers()
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		fmt.Fprint(w, err)
		return
	}

	fmt.Fprint(w, bodystring1+bodystring2)
}
