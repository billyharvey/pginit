package pginit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"

	"code.google.com/p/gopass"
	"github.com/jmoiron/sqlx"
)

type Postgres struct {
	Connection PostgresConnection
	Tables     []PostgresTables
}

type PostgresConnection struct {
	Host string
	Port string
	Maxo string
	Maxi string
	Data string
	Mode string
	User string
	Pass string
}

type PostgresTables struct {
	Table    string
	Columns  []string
	Checks   []string
	Uniques  []string
	Indexes  []string
	Defaults []string
}

func Init(postgres_json string) *sqlx.DB {
	var err error
	var db *sqlx.DB
	var pg Postgres
	var maxo, maxi int
	a, err := ioutil.ReadFile(postgres_json)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(a, &pg)
	if err != nil {
		panic(err)
	}
	if pg.Connection.Port == "" {
		pg.Connection.Port = "5432"
	}
	if pg.Connection.Mode == "" {
		pg.Connection.Mode = "require"
	}
	if pg.Connection.Maxo == "" {
		maxo = 10
	} else {
		maxo, err = strconv.Atoi(pg.Connection.Maxo)
		if err != nil {
			panic(err)
		}
	}
	if pg.Connection.Maxi == "" {
		maxi = 1
	} else {
		maxi, err = strconv.Atoi(pg.Connection.Maxi)
		if err != nil {
			panic(err)
		}
	}
	db, err = sqlx.Connect("postgres",
		"host="+pg.Connection.Host+
			" port="+pg.Connection.Port+
			" user="+pg.Connection.User+
			" password="+pg.Connection.Pass+
			" dbname="+pg.Connection.Data+
			" sslmode="+pg.Connection.Mode)
	if err != nil {
		if err.Error() == "pq: password authentication failed for user \""+pg.Connection.User+"\"" || err.Error() == "pq: database \""+pg.Connection.Data+"\" does not exist" {
			password, err := gopass.GetPass("Enter postgres password to setup database: ")
			if err != nil {
				panic(err)
			}
			db, err := sqlx.Connect("postgres",
				"host="+pg.Connection.Host+
					" port="+pg.Connection.Port+
					" user=postgres"+
					" password="+password+
					" dbname=postgres"+
					" sslmode="+pg.Connection.Mode)
			if err != nil {
				panic(err)
			}
			_, err = db.Exec("CREATE USER " + pg.Connection.User + " PASSWORD '" + pg.Connection.Pass + "'")
			if err != nil {
				panic(err)
			}
			_, err = db.Exec("CREATE DATABASE " + pg.Connection.Data + " ENCODING 'UTF8' TEMPLATE template0")
			if err != nil {
				panic(err)
			}
			_, err = db.Exec("GRANT ALL ON DATABASE " + pg.Connection.Data + " TO " + pg.Connection.User)
			if err != nil {
				panic(err)
			}
			fmt.Println("Database setup completed.  Restart application.")
		} else {
			if err != nil {
				panic(err)
			}
		}
		os.Exit(0)
	}
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(maxo)
	db.SetMaxIdleConns(maxi)
	for _, table := range pg.Tables {
		_, err = db.Exec("CREATE TABLE IF NOT EXISTS " + table.Table + "()")
		if err != nil {
			panic(err)
		}
		var count int
		for _, column := range table.Columns {
			columnName := strings.Split(column, " ")[0]
			err = db.Get(&count, "SELECT count(*) FROM pg_attribute WHERE attrelid='"+table.Table+"'::regclass AND attname='"+columnName+"' AND NOT attisdropped")
			if err != nil {
				panic(err)
			}
			if count == 0 {
				_, err = db.Exec("ALTER TABLE " + table.Table + " ADD " + column)
				if err != nil {
					panic(err)
				}
			}
		}
		for _, check := range table.Checks {
			checkName := "check_" + Hash([]byte(pg.Connection.Host + ":" + pg.Connection.Data + ":" + table.Table + ":" + check))[0:16]
			err = db.Get(&count, "SELECT count(*) FROM pg_constraint where conname='"+checkName+"'")
			if err != nil {
				panic(err)
			}
			if count == 0 {
				_, err = db.Exec("ALTER TABLE " + table.Table + " ADD CONSTRAINT " + checkName + " CHECK (" + check + ")")
				if err != nil {
					panic(err)
				}
			}
		}
		for _, unique := range table.Uniques {
			checkName := "check_" + Hash([]byte(pg.Connection.Host + ":" + pg.Connection.Data + ":" + table.Table + ":" + unique))[0:16]
			err = db.Get(&count, "SELECT count(*) FROM pg_constraint where conname='"+checkName+"'")
			if err != nil {
				panic(err)
			}
			if count == 0 {
				_, err = db.Exec("ALTER TABLE " + table.Table + " ADD CONSTRAINT " + checkName + " UNIQUE (" + unique + ")")
				if err != nil {
					panic(err)
				}
			}
		}
		for _, dbIndex := range table.Indexes {
			dbIndexName := "index_" + Hash([]byte(pg.Connection.Host + ":" + pg.Connection.Data + ":" + table.Table + ":" + dbIndex))[0:16]
			err = db.Get(&count, "SELECT count(*) FROM pg_class where relname='"+dbIndexName+"'")
			if err != nil {
				panic(err)
			}
			if count == 0 {
				_, err = db.Exec("CREATE INDEX " + dbIndexName + " ON " + table.Table + " (" + dbIndex + ")")
				if err != nil {
					panic(err)
				}
				if err != nil {
					panic(err)
				}
			}
		}
		for _, def := range table.Defaults {
			_, err = db.Exec("ALTER TABLE " + table.Table + " ALTER COLUMN " + strings.Split(def, " ")[0] + " SET DEFAULT " + strings.Join(strings.Split(def, " ")[1:], " "))
			if err != nil {
				panic(err)
			}
		}
	}
	return db
}
