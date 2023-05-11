package main

import (
	"context"
	"fmt"

	"github.com/evanj/hacks/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func main() {
	fmt.Println("hstore demo; starting postgres instance ...")
	instance, err := postgrestest.NewInstance()
	if err != nil {
		panic(err)
	}
	defer instance.Close()

	cfg, err := pgx.ParseConfig(instance.URL())
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
}
