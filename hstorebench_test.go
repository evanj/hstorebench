package main

import (
	"context"
	"errors"
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

var errHstoreDoesNotExist = errors.New("postgres type hstore does not exist (the extension may not be loaded)")

func registerHstore(ctx context.Context, conn *pgx.Conn) error {
	// get the hstore OID: it varies because hstore is an extension and not built-in
	var hstoreOID uint32
	err := conn.QueryRow(ctx, `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return errHstoreDoesNotExist
		}
		return err
	}

	conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})
	return nil
}

func TestRegisterHstore(t *testing.T) {
	postgresURL := postgrestest.New(t)
	ctx := context.Background()
	pgxConn, err := pgx.Connect(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pgxConn.Close(ctx) })

	pgt, ok := pgxConn.TypeMap().TypeForName("hstore")
	if !(pgt == nil && !ok) {
		t.Fatalf("did not expect hstore to be registered; TypeForName returned: pgt=%#v ok=%#v",
			pgt, ok)
	}

	// extension not registered
	err = registerHstore(ctx, pgxConn)
	if err != errHstoreDoesNotExist {
		t.Errorf("extension not registered; expected errHstoreDoesNotExist, got err=%#v", err)
	}

	_, err = pgxConn.Exec(ctx, "create extension hstore")
	if err != nil {
		t.Fatal(err)
	}
	err = registerHstore(ctx, pgxConn)
	if err != nil {
		t.Fatalf("extension registered but got err=%#v", err)
	}
	pgt, ok = pgxConn.TypeMap().TypeForName("hstore")
	if !(pgt != nil && ok) {
		t.Fatalf("hstore must be registered; TypeForName returned: pgt=%#v ok=%#v",
			pgt, ok)
	}
	if pgt.Codec.PreferredFormat() != pgtype.BinaryFormatCode {
		t.Errorf("expected preferred format to be binary, was %d", pgt.Codec.PreferredFormat())
	}
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

	// create a pgx connection with hstore registered as an explicit type; uses binary format
	pgxConnHstoreRegistered, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	registerHstore(ctx, pgxConnHstoreRegistered)
	b.Cleanup(func() { pgxConnHstoreRegistered.Close(context.Background()) })

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

		_, err = pgxConn.Exec(ctx, "INSERT INTO benchmark VALUES ($1);", rowBuilder.String())
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
	// calls rows.Values() on a connections with hstore registered which returns a pgtype.Hstore
	pgxValuesHstoreRegistered := func() error {
		rows, err := pgxConnHstoreRegistered.Query(ctx, query)
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
			// values[0] is of type pgtype.Hstore
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
	b.Run("pgxValuesHstoreRegistered", timeIt(pgxValuesHstoreRegistered))
	b.Run("pgxScanHstore", timeIt(pgxScanHstore))
	b.Run("pgxsqlScanHstore", timeIt(sqlScanHstore))

	// test pgx.Scan with the registered codec with all query modes
	// some use the binary protocol and some use the text protocol
	queryModes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
		pgx.QueryExecModeSimpleProtocol,
	}
	for _, queryMode := range queryModes {
		b.Run("pgxScanRegistered/mode="+queryMode.String(), timeIt(func() error {
			scanHstore := pgtype.Hstore{}
			scanArgs := []interface{}{&scanHstore}
			rows, err := pgxConnHstoreRegistered.Query(ctx, query, queryMode)
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
		}))
	}
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
