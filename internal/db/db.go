/*
Copyright 2026 The pgmq-cli Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DB is a small wrapper around a pgx connection.
type DB struct {
	Conn *pgx.Conn
}

func Connect(ctx context.Context, connStr string) (*DB, error) {
	connStr = normalizeConnString(connStr)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}
	return &DB{Conn: conn}, nil
}

func (db *DB) Close(ctx context.Context) error {
	if db == nil || db.Conn == nil {
		return nil
	}
	return db.Conn.Close(ctx)
}

func normalizeConnString(connStr string) string {
	if connStr == "" {
		return connStr
	}
	if strings.Contains(connStr, "://") {
		return connStr
	}
	if strings.Contains(connStr, ";") {
		replacer := strings.NewReplacer(
			";", " ",
			"Host=", "host=",
			"Port=", "port=",
			"Username=", "user=",
			"User=", "user=",
			"Password=", "password=",
			"Database=", "dbname=",
			"SSLMode=", "sslmode=",
		)
		return strings.TrimSpace(replacer.Replace(connStr))
	}
	return connStr
}
