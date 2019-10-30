package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"

	v4 "github.com/jackc/pgx/v4"
)

type fieldsFunc func(*Model) []interface{}

func getConn() *v4.Conn {
	config, err := v4.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := v4.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func query(ctx context.Context, conn *v4.Conn, sql string) v4.Rows {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		log.Fatal(err)
	}
	return rows
}

func runTest(ctx context.Context, rows v4.Rows, getModelFields fieldsFunc) {
	var models []*Model

	for rows.Next() {
		model := Model{}
		modelFields := getModelFields(&model)
		err := rows.Scan(modelFields...)
		if err != nil {
			log.Fatal(err)
		}
		models = append(models, &model)

		if model.ID%100000 == 0 {
			fmt.Print(".")
		}
	}

	// Any errors encountered by rows.Next or rows.Scan will be returned here
	if rows.Err() != nil {
		log.Fatal(rows.Err())
	}

	// probably overkill
	runtime.KeepAlive(models)
}

func resetInsertTable(conn *v4.Conn) {
	sql := `
		DROP TABLE IF EXISTS perf2;
		CREATE TABLE perf2 (
		    j jsonb,
		    id integer,
		    name character varying,
		    status character varying,
		    last_updated_at timestamp without time zone,
		    last_modified_at timestamp without time zone,
		    num integer,
		    num2 integer,
		    updated_at timestamp without time zone,
		    created_at timestamp without time zone,
		    jumbled_at timestamp without time zone,
		    secret_code character varying,
		    entries integer
		);
	`

	conn.Exec(context.Background(), sql)
}

func BenchmarkInsertJson(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	resetInsertTable(conn)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		model := createModel(n + 1)
		_, err := conn.Exec(ctx, `insert into perf2 (j) values ($1)`, model)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkInsertNormalColumns(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	resetInsertTable(conn)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		model := createModel(n + 1)
		_, err := conn.Exec(ctx, `
			insert into perf2
				(id, name, status, last_updated_at, last_modified_at,
				 num, num2, updated_at, created_at, jumbled_at, secret_code, entries)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			model.ID, model.Name, model.Status, model.LastUpdatedAt, model.LastModifiedAt,
			model.Num, model.Num2, model.UpdatedAt, model.CreatedAt, model.JumbledAt,
			model.SecretCode, model.Entries,
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkUpdate2JsonbFields(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := conn.Exec(ctx, `update perf set j = jsonb_set(jsonb_set(j, '{name}', to_jsonb($1::text)), '{num}', $3) where id = $2`, fmt.Sprintf("UpdateName:%d", n), n+1, n)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkUpdate2NormalColumns(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := conn.Exec(ctx, `update perf set name = $1, num = $3 where id = $2`, fmt.Sprintf("UpdateName:%d", n), n+1, n)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkSelectAndParseJsonb(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := `select j from perf`

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows := query(ctx, conn, sql)
		defer rows.Close()
		runTest(ctx, rows, func(m *Model) []interface{} {
			return []interface{}{m}
		})
	}

	fmt.Println()
}

func BenchmarkSelectAndParseNormalColumns(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := `
		select
			id, name, status, last_updated_at, last_modified_at,
		  num, num2, updated_at, created_at, jumbled_at, secret_code, entries
		from perf
	`

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows := query(ctx, conn, sql)
		defer rows.Close()
		runTest(ctx, rows, func(m *Model) []interface{} {
			return []interface{}{
				&m.ID,
				&m.Name,
				&m.Status,
				&m.LastUpdatedAt,
				&m.LastModifiedAt,
				&m.Num,
				&m.Num2,
				&m.UpdatedAt,
				&m.CreatedAt,
				&m.JumbledAt,
				&m.SecretCode,
				&m.Entries,
			}
		})
	}

	fmt.Println()
}

func BenchmarkSelectAndParseJsonbColumns(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := `
		select
			(j->>'id')::int,
			j->>'name',
			j->>'status',
			((j->>'last_updated_at')::text)::timestamp,
			((j->>'last_modified_at')::text)::timestamp,
			(j->>'num')::int,
			(j->>'num2')::int,
			((j->>'updated_at')::text)::timestamp,
			((j->>'created_at')::text)::timestamp,
			((j->>'jumbled_at')::text)::timestamp,
			j->>'secret_code',
			(j->>'entries')::int
		from perf
	`

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows := query(ctx, conn, sql)
		defer rows.Close()
		runTest(ctx, rows, func(m *Model) []interface{} {
			return []interface{}{
				&m.ID,
				&m.Name,
				&m.Status,
				&m.LastUpdatedAt,
				&m.LastModifiedAt,
				&m.Num,
				&m.Num2,
				&m.UpdatedAt,
				&m.CreatedAt,
				&m.JumbledAt,
				&m.SecretCode,
				&m.Entries,
			}
		})
	}

	fmt.Println()
}

func BenchmarkSelectAndParseSingleJsonbField(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := `select (j->>'id')::int from perf`

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows := query(ctx, conn, sql)
		defer rows.Close()
		runTest(ctx, rows, func(m *Model) []interface{} {
			return []interface{}{
				&m.ID,
			}
		})
	}

	fmt.Println()
}

func BenchmarkSelectAndParseSingleNormalColumn(b *testing.B) {
	conn := getConn()
	ctx := context.Background()
	defer conn.Close(ctx)

	sql := `select id from perf`

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows := query(ctx, conn, sql)
		defer rows.Close()
		runTest(ctx, rows, func(m *Model) []interface{} {
			return []interface{}{
				&m.ID,
			}
		})
	}

	fmt.Println()
}
