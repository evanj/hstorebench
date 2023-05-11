package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"strings"
	"testing"

	"github.com/evanj/hacks/postgrestest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
)

const numRows = 10000
const maxKVPairsPerRow = 10
const rngSeed = 123 // to try to make tests repeatable

func genString(rng *mathrand.Rand) string {
	s := fmt.Sprintf("%016x", rng.Int63())
	return s[0 : 1+rng.Intn(len(s)-1)]
}

func BenchmarkHstore(b *testing.B) {
	b.Log("starting postgres instance")
	postgresURL := postgrestest.New(b)

	cfg, err := pgx.ParseConfig(postgresURL)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	pgxConn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	b.Cleanup(func() { pgxConn.Close(context.Background()) })

	sqlDB := stdlib.OpenDB(*cfg)
	err = sqlDB.Ping()
	if err != nil {
		panic(err)
	}
	b.Cleanup(func() { sqlDB.Close() })

	b.Logf("filling benchmark table numRows=%d maxKVPairsPerRow=%d ...\n", numRows, maxKVPairsPerRow)
	_, err = pgxConn.Exec(ctx, "CREATE EXTENSION hstore")
	if err != nil {
		panic(err)
	}
	_, err = pgxConn.Exec(ctx, "CREATE TABLE benchmark (kv HSTORE)")
	if err != nil {
		panic(err)
	}

	rng := mathrand.New(mathrand.NewSource(rngSeed))
	totalKVBytes := 0

	// generate each row
	rowBuilder := &strings.Builder{}
	for i := 0; i < numRows; i++ {
		rowBuilder.Reset()
		rowBuilder.WriteByte('\'')

		// generate kv pairs
		numPairs := 1 + rng.Intn(maxKVPairsPerRow-1)
		for j := 0; j < numPairs; j++ {
			keyString := genString(rng)
			valueString := genString(rng)

			if j != 0 {
				rowBuilder.WriteByte(',')
			}
			rowBuilder.WriteString(keyString)
			rowBuilder.WriteString("=>")
			rowBuilder.WriteString(valueString)

			totalKVBytes += len(keyString) + len(valueString)
		}

		rowBuilder.WriteByte('\'')

		insert := fmt.Sprintf("INSERT INTO benchmark VALUES (%s);", rowBuilder.String())
		_, err = pgxConn.Exec(ctx, insert)
		if err != nil {
			panic(err)
		}
	}
	b.Logf("   generated %d total KV bytes\n", totalKVBytes)

	const query = "SELECT kv FROM benchmark"
	pgxRawValues := func() error {
		rows, err := pgxConn.Query(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			values := rows.RawValues()
			if len(values) != 1 {
				return fmt.Errorf("unexpected values: %#v", values)
			}
		}
		return rows.Err()
	}

	// calls rows.Values() which returns a type string
	pgxValuesString := func() error {
		rows, err := pgxConn.Query(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				return err
			}
			if len(values) != 1 {
				return fmt.Errorf("unexpected values: %#v", values)
			}
			// values[0] is of type string() not hstore
			// panic(fmt.Sprintf("%#v %T", values[0], values[0]))
		}
		return rows.Err()
	}
	pgxScanHstore := func() error {
		scanHstore := pgtype.Hstore{}
		scanArgs := []interface{}{&scanHstore}
		rows, err := pgxConn.Query(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			err := rows.Scan(scanArgs...)
			if err != nil {
				return err
			}
			if len(scanHstore) == 0 {
				return fmt.Errorf("unexpected empty hstore: %#v", scanHstore)
			}
		}
		return rows.Err()
	}
	sqlScanHstore := func() error {
		scanHstore := pgtype.Hstore{}
		scanArgs := []interface{}{&scanHstore}
		rows, err := sqlDB.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			err := rows.Scan(scanArgs...)
			if err != nil {
				return err
			}
			if len(scanHstore) == 0 {
				return fmt.Errorf("unexpected empty hstore: %#v", scanHstore)
			}
		}
		return rows.Err()
	}

	b.Run("pgxRawValues", timeIt(pgxRawValues))
	b.Run("pgxValuesString", timeIt(pgxValuesString))
	b.Run("pgxScanHstore", timeIt(pgxScanHstore))
	b.Run("pgxsqlScanHstore", timeIt(sqlScanHstore))
}

func timeIt(f func() error) func(b *testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := f()
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
