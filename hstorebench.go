package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/evanj/hacks/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var errHstoreDoesNotExist = errors.New("postgres type hstore does not exist (the extension may not be loaded)")

// queryHstoreOID returns the Postgres Object Identifer (OID) for the "hstore" type. This must be
// done for each separate Postgres database, since the OID can be different. It returns
// errHstoreDoesNotExist if the row does not exist.
func queryHstoreOID(ctx context.Context, conn *pgx.Conn) (uint32, error) {
	// get the hstore OID: it varies because hstore is an extension and not built-in
	var hstoreOID uint32
	err := conn.QueryRow(ctx, `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, errHstoreDoesNotExist
		}
		return 0, err
	}
	return hstoreOID, nil
}

// queryHstoreOIDSQL returns the Postgres Object Identifer (OID) for the "hstore" type. This must be
// done for each separate Postgres database, since the OID can be different. It returns
// errHstoreDoesNotExist if the row does not exist.
func queryHstoreOIDSQL(ctx context.Context, db *sql.DB) (uint32, error) {
	// get the hstore OID: it varies because hstore is an extension and not built-in
	var hstoreOID uint32
	err := db.QueryRowContext(ctx, `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, errHstoreDoesNotExist
		}
		return 0, err
	}
	return hstoreOID, nil
}

// registerHstoreTypeMap registers the hstore type with typeMap. It uses conn to query for
func registerHstoreTypeMap(hstoreOID uint32, typeMap *pgtype.Map) {
	typeMap.RegisterType(&pgtype.Type{Codec: pgtype.HstoreCodec{}, Name: "hstore", OID: hstoreOID})
}

// registerHstore registers the hstore type with this connection's default type map. A connection
// can only access a specific database, so
func registerHstore(ctx context.Context, conn *pgx.Conn) error {
	hstoreOID, err := queryHstoreOID(ctx, conn)
	if err != nil {
		return err
	}
	registerHstoreTypeMap(hstoreOID, conn.TypeMap())
	return nil
}

func main() {
	fmt.Println("hstore demo; starting postgres instance ...")
	instance, err := postgrestest.NewInstanceWithOptions(postgrestest.Options{ListenOnLocalhost: true})
	if err != nil {
		panic(err)
	}
	defer instance.Close()

	cfg, err := pgx.ParseConfig("postgresql://127.0.0.1:5432/postgres")
	// cfg, err := pgx.ParseConfig(instance.URL())
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pgxConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	defer pgxConn.Close(context.Background())

	// sqlDB := stdlib.OpenDB(*cfg)
	// err = sqlDB.Ping()
	// if err != nil {
	// 	panic(err)
	// }
	// defer sqlDB.Close()

	_, err = pgxConn.Exec(ctx, "CREATE EXTENSION hstore")
	if err != nil {
		panic(err)
	}

	// var hstoreOID uint32
	// err = pgxConn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	// if err != nil {
	// 	panic(err)
	// }
	// pgxConn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})

	rows, err := pgxConn.Query(ctx, "SELECT CAST('' AS hstore) UNION ALL SELECT CAST(NULL AS hstore)")
	// rows, err := pgxConn.Query(ctx, "SELECT CAST('' AS hstore) UNION ALL SELECT CAST(NULL AS hstore)", pgx.QueryExecModeExec)
	if err != nil {
		panic(err)
	}
	var h pgtype.Hstore
	for rows.Next() {
		err = rows.Scan(&h)
		if err != nil {
			panic(err)
		}
		fmt.Printf("row h=%#v\n", h)
	}
	if rows.Err() != nil {
		panic(rows.Err())
	}
	fmt.Println("===")

	// _, err = pgxConn.Exec(ctx, "CREATE TABLE foo (kv HSTORE);")
	// if err != nil {
	// 	panic(err)
	// }
	// // const hstoreArg = `"key"=>"line1` + "\n" + `line2"`
	// const hstoreArg = "\"0\"=>\"0\", \"1\"=>\"0\", \"2\"=>\"\n\""
	// const hstoreArg = "\"0\"=>\"0\", \"1\"=>\"0\", \"00\"=>\"a嘅b\""
	const hstoreArg = "\"0\"=>\"0\", \"1\"=>\"0\", \"00\"=>\"a😅b\""
	// UTF-8 encodings ending in \x85 cause a problem, those ending in something else don't?
	// POSTGRES bug!
	// const hstoreArg = "\"0\"=>\"0\", \"1\"=>\"0\", \"00\"=>\"a😄b\""
	// const hstoreArg = "\"0\"=>\"0\", \"1\"=>\"0\", \"00\"=>\"嘅\""
	fmt.Println(hstoreArg)
	// _, err = pgxConn.Exec(ctx, "INSERT INTO foo VALUES ($1);", pgx.QueryExecModeExec, hstoreArg)
	// if err != nil {
	// 	panic(err)
	// }
	var h2 pgtype.Hstore
	err = h2.Scan(hstoreArg)
	if err != nil {
		panic(err)
	}
	v, err := h2.Value()
	if err != nil {
		panic(err)
	}
	fmt.Printf("wtf %s %#v", v, v)
	rows, err = pgxConn.Query(ctx, "SELECT $1::hstore", h2)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var h pgtype.Hstore
		err = rows.Scan(&h)
		if err != nil {
			panic(err)
		}
		v, err := h.Value()
		if err != nil {
			panic(err)
		}
		fmt.Printf("row h=%#v v=%s\n", h, v)
	}
	if rows.Err() != nil {
		panic(rows.Err())
	}

	rows, err = pgxConn.Query(ctx, "SELECT $1::hstore", hstoreArg)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var h pgtype.Hstore
		err = rows.Scan(&h)
		if err != nil {
			panic(err)
		}
		v, err := h.Value()
		if err != nil {
			panic(err)
		}
		fmt.Printf("row h=%#v v=%s\n", h, v)
	}
	if rows.Err() != nil {
		panic(rows.Err())
	}

	pgxConnCodec, err := pgx.Connect(ctx, instance.URL())
	if err != nil {
		panic(err)
	}
	defer pgxConnCodec.Close(ctx)
	err = registerHstore(ctx, pgxConnCodec)
	if err != nil {
		panic(err)
	}
	rows, err = pgxConnCodec.Query(ctx, "SELECT $1::hstore", h2)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var h pgtype.Hstore
		err = rows.Scan(&h)
		if err != nil {
			panic(err)
		}
		v, err := h.Value()
		if err != nil {
			panic(err)
		}
		fmt.Printf("row h=%#v v=%s\n", h, v)
	}
	if rows.Err() != nil {
		panic(rows.Err())
	}
}
