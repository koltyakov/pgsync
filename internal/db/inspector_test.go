package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestGetTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	inspector := NewInspector(db, db, "public")

	tests := []struct {
		name        string
		mockSetup   func()
		expected    []string
		expectError bool
	}{
		{
			name: "returns tables successfully",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"table_name"}).
					AddRow("users").
					AddRow("orders").
					AddRow("products")
				mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
					WithArgs("public").
					WillReturnRows(rows)
			},
			expected:    []string{"users", "orders", "products"},
			expectError: false,
		},
		{
			name: "returns empty list when no tables",
			mockSetup: func() {
				rows := sqlmock.NewRows([]string{"table_name"})
				mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
					WithArgs("public").
					WillReturnRows(rows)
			},
			expected:    nil,
			expectError: false,
		},
		{
			name: "handles query error",
			mockSetup: func() {
				mock.ExpectQuery("SELECT table_name FROM information_schema.tables").
					WithArgs("public").
					WillReturnError(sql.ErrConnDone)
			},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.mockSetup()

			tables, err := inspector.GetTables(context.Background())

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if len(tables) != len(tt.expected) {
				t.Errorf("expected %d tables, got %d", len(tt.expected), len(tables))
			}

			for i, table := range tables {
				if table != tt.expected[i] {
					t.Errorf("expected table %q, got %q", tt.expected[i], table)
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestGetTableInfo(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Enable MatchExpectationsInOrder(false) to handle concurrent queries
	mock.MatchExpectationsInOrder(false)

	inspector := NewInspector(db, db, "public")

	t.Run("returns complete table info", func(t *testing.T) {
		// Note: GetTableInfo runs queries concurrently, so we use AnyOrder mode
		// Mock columns query
		colRows := sqlmock.NewRows([]string{"column_name"}).
			AddRow("id").
			AddRow("name").
			AddRow("email")
		mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
			WithArgs("public", "users").
			WillReturnRows(colRows)

		// Mock primary key query - pq.QuoteIdentifier quotes both schema and table
		pkRows := sqlmock.NewRows([]string{"attname"}).
			AddRow("id")
		mock.ExpectQuery("SELECT a.attname FROM pg_index").
			WithArgs(`"public"."users"`).
			WillReturnRows(pkRows)

		// Mock row count query - now uses COUNT(*)
		mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "public"\."users"`).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1000))

		info, err := inspector.GetTableInfo(context.Background(), "users")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if info.Name != "users" {
			t.Errorf("expected name 'users', got %q", info.Name)
		}
		if info.Schema != "public" {
			t.Errorf("expected schema 'public', got %q", info.Schema)
		}
		if len(info.Columns) != 3 {
			t.Errorf("expected 3 columns, got %d", len(info.Columns))
		}
		if len(info.PrimaryKey) != 1 || info.PrimaryKey[0] != "id" {
			t.Errorf("expected primary key [id], got %v", info.PrimaryKey)
		}
		if info.RowCount != 1000 {
			t.Errorf("expected row count 1000, got %d", info.RowCount)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unfulfilled expectations: %v", err)
		}
	})
}

func TestNewInspector(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer func() { _ = db.Close() }()

	inspector := NewInspector(db, db, "test_schema")

	if inspector == nil {
		t.Fatal("expected non-nil inspector")
	}
	if inspector.schema != "test_schema" {
		t.Errorf("expected schema 'test_schema', got %q", inspector.schema)
	}
}
