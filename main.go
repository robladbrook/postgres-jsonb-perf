package main

import (
	"context"
	"fmt"
	"os"
	"time"

	v4 "github.com/jackc/pgx/v4"
)

// Schema:
//
// CREATE TABLE perf (
//     j jsonb,
//     id integer,
//     name character varying,
//     status character varying,
//     last_updated_at timestamp without time zone,
//     last_modified_at timestamp without time zone,
//     num integer,
//     num2 integer,
//     updated_at timestamp without time zone,
//     created_at timestamp without time zone,
//     jumbled_at timestamp without time zone,
//     secret_code character varying,
//     entries integer
// );
//
// CREATE INDEX idx_perf_id ON perf(id);

// Model represents an example table of data.
type Model struct {
	ID             int        `json:"id"`
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	LastUpdatedAt  *time.Time `json:"last_updated_at"`
	LastModifiedAt *time.Time `json:"last_modified_at"`
	Num            *int       `json:"num"`
	Num2           *int       `json:"num2"`
	UpdatedAt      *time.Time `json:"updated_at"`
	CreatedAt      *time.Time `json:"created_at"`
	JumbledAt      *time.Time `json:"jumbled_at"`
	SecretCode     string     `json:"secret_code"`
	Entries        int        `json:"entries"`
}

func createModel(i int) *Model {
	now := time.Now()
	ii := i + 7
	pf := &Model{
		ID:             i,
		Name:           fmt.Sprintf("MyName:%d", i),
		Status:         fmt.Sprintf("MyStatus:%d", i),
		LastUpdatedAt:  &now,
		LastModifiedAt: &now,
		Num:            &ii,
		Num2:           &ii,
		UpdatedAt:      &now,
		CreatedAt:      &now,
		JumbledAt:      &now,
		SecretCode:     fmt.Sprintf("MyCode:%d", i),
		Entries:        i + 10,
	}

	return pf
}

// Seed the database with 2 million entries.
func main() {
	conn, err := v4.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	for i := 0; i < 2000000; i++ {
		if i%100000 == 0 {
			fmt.Println(i)
		}

		pf := createModel(i)
		conn.Exec(context.Background(),
			`insert into perf (
			   j, id, name, status, last_updated_at, last_modified_at, num, num2, updated_at,
			   created_at, jumbled_at, secret_code, entries)
		   values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			pf, pf.ID, pf.Name, pf.Status, pf.LastUpdatedAt, pf.LastModifiedAt,
			pf.Num, pf.Num2, pf.UpdatedAt, pf.CreatedAt, pf.JumbledAt, pf.SecretCode, pf.Entries)
	}
}
