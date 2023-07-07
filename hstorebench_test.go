package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"strings"
	"testing"

	"github.com/evanj/hacks/postgrestest"
	"github.com/evanj/pgxtypefaster"
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

// HstoreSQLBinary uses the binary protocol with the database/sql API.
// This is a proof-of-concept hack more than a good idea.
type HstoreSQLBinary struct {
	pgxtypefaster.Hstore
}

var pgxfasterBinaryScanPlan = pgxtypefaster.HstoreCodec{}.PlanScan(
	nil, 0, pgtype.BinaryFormatCode, (*pgxtypefaster.Hstore)(nil))

// Scan implements the database/sql Scanner interface.
func (h *HstoreSQLBinary) Scan(src any) error {
	return pgxfasterBinaryScanPlan.Scan([]byte(src.(string)), &h.Hstore)
}

func BenchmarkHstore(b *testing.B) {
	b.Log("starting postgres instance")

	instance, err := postgrestest.NewInstanceWithOptions(postgrestest.Options{ListenOnLocalhost: true})
	if err != nil {
		b.Fatal(err)
	}
	defer instance.Close()
	postgresURL := instance.URL()

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
	err = registerHstore(ctx, pgxConnHstoreRegistered)
	if err != nil {
		panic(err)
	}
	b.Cleanup(func() { pgxConnHstoreRegistered.Close(context.Background()) })

	// create a pgx connection with hstore registered as an explicit type; uses binary format
	pgxConnFasterHstoreRegistered, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	err = pgxtypefaster.RegisterHstore(ctx, pgxConnFasterHstoreRegistered)
	if err != nil {
		panic(err)
	}
	b.Cleanup(func() { pgxConnFasterHstoreRegistered.Close(context.Background()) })

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

	hstoreOID, err := queryHstoreOIDSQL(ctx, sqlDB)
	if err != nil {
		panic(err)
	}

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
	sqlScanHstoreFaster := func() error {
		scanHstore := pgxtypefaster.Hstore{}
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
	sqlScanHstoreFasterRawBinary := func() error {
		hstorePGType := &pgtype.Type{Codec: pgxtypefaster.HstoreCodec{}, Name: "hstore", OID: hstoreOID}

		var scanHstore pgxtypefaster.Hstore
		scanArgs := []interface{}{&scanHstore}
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		return conn.Raw(func(driverConn any) error {
			pgxConn := driverConn.(*stdlib.Conn).Conn()
			pgxConn.TypeMap().RegisterType(hstorePGType)
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
		})
	}

	b.Run("pgxRawValues", timeIt(pgxRawValues))
	b.Run("pgxValuesString", timeIt(pgxValuesString))
	b.Run("pgxValuesHstoreRegistered", timeIt(pgxValuesHstoreRegistered))
	b.Run("pgxsqlScanHstore", timeIt(sqlScanHstore))
	b.Run("pgxsqlScanHstoreFaster", timeIt(sqlScanHstoreFaster))
	b.Run("pgxsqlScanHstoreBinaryRawConn", timeIt(sqlScanHstoreFasterRawBinary))

	// test pgx.Scan with the registered codec with all query modes
	// some use the binary protocol and some use the text protocol
	queryModes := []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement,
		pgx.QueryExecModeCacheDescribe,
		pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec,
		pgx.QueryExecModeSimpleProtocol,
	}
	connConfigs := []struct {
		label      string
		conn       *pgx.Conn
		newScanArg func() any
		scanArgLen func(arg any) int
	}{
		{
			"default",
			pgxConn,
			func() any { return &pgtype.Hstore{} },
			func(scanArg any) int { return len(*scanArg.(*pgtype.Hstore)) },
		},
		{
			"hstore_registered",
			pgxConnHstoreRegistered,
			func() any { return &pgtype.Hstore{} },
			func(scanArg any) int { return len(*scanArg.(*pgtype.Hstore)) },
		},
		{
			"faster_hstore_registered",
			pgxConnFasterHstoreRegistered,
			func() any { return &pgxtypefaster.Hstore{} },
			func(scanArg any) int { return len(*scanArg.(*pgxtypefaster.Hstore)) },
		},
	}
	for _, connConfig := range connConfigs {
		for _, queryMode := range queryModes {
			scanArgs := []interface{}{connConfig.newScanArg()}

			label := fmt.Sprintf("pgxScan/%s/mode=%s", connConfig.label, queryMode)
			b.Run(label, timeIt(func() error {
				rows, err := connConfig.conn.Query(ctx, query, queryMode)
				if err != nil {
					return err
				}
				for rows.Next() {
					err := rows.Scan(scanArgs...)
					if err != nil {
						return err
					}
					if connConfig.scanArgLen(scanArgs[0]) == 0 {
						return fmt.Errorf("unexpected empty hstore: %#v", scanArgs[0])
					}
				}
				return rows.Err()
			}))
		}
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
