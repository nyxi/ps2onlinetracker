CREATE TABLE character_names ( id bigint PRIMARY KEY, name text, faction_id integer, world_id integer );
CREATE TABLE character_outfit ( character_id bigint PRIMARY KEY, outfit_id bigint, updated timestamp );
CREATE TABLE faction_names ( id integer PRIMARY KEY, name text, tag text );
CREATE TABLE online_players ( character_id bigint PRIMARY KEY );
CREATE TABLE outfit_names ( id bigint PRIMARY KEY, alias text, name text, faction_id integer, world_id integer );
CREATE TABLE world_names ( id integer PRIMARY KEY, name text );
INSERT INTO faction_names VALUES (1, 'Vanu Sovereignty', 'VS');
INSERT INTO faction_names VALUES (2, 'New Conglomerate', 'NC');
INSERT INTO faction_names VALUES (3, 'Terran Republic', 'TR');
INSERT INTO world_names VALUES (25, 'Briggs');
INSERT INTO world_names VALUES (10, 'Miller');
INSERT INTO world_names VALUES (13, 'Cobalt');
INSERT INTO world_names VALUES (1, 'Connery');
INSERT INTO world_names VALUES (17, 'Emerald');
