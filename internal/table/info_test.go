package table

import "testing"

func TestInfo_HasColumn(t *testing.T) {
	info := &Info{
		Name:    "test_table",
		Schema:  "public",
		Columns: []string{"id", "name", "email", "created_at", "updated_at"},
	}

	tests := []struct {
		column   string
		expected bool
	}{
		{"id", true},
		{"name", true},
		{"email", true},
		{"created_at", true},
		{"updated_at", true},
		{"nonexistent", false},
		{"ID", true},    // Case insensitive
		{"NAME", true},  // Case insensitive
		{"EMAIL", true}, // Case insensitive
		{"", false},
	}

	for _, test := range tests {
		result := info.HasColumn(test.column)
		if result != test.expected {
			t.Errorf("HasColumn(%q) = %v, expected %v", test.column, result, test.expected)
		}
	}
}

func TestInfo_EstimatedWork(t *testing.T) {
	tests := []struct {
		name        string
		info        *Info
		expectedMin int64
		expectedMax int64
	}{
		{
			name: "small table with primary key",
			info: &Info{
				Name:       "small_table",
				Columns:    []string{"id", "name"},
				PrimaryKey: []string{"id"},
				RowCount:   100,
			},
			expectedMin: 100,
			expectedMax: 200,
		},
		{
			name: "large table with primary key",
			info: &Info{
				Name:       "large_table",
				Columns:    []string{"id", "name", "email", "data"},
				PrimaryKey: []string{"id"},
				RowCount:   10000,
			},
			expectedMin: 10000,
			expectedMax: 10100,
		},
		{
			name: "table without primary key",
			info: &Info{
				Name:       "no_pk_table",
				Columns:    []string{"name", "data"},
				PrimaryKey: []string{},
				RowCount:   1000,
			},
			expectedMin: 2000, // Should be doubled due to no PK
			expectedMax: 2100,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			work := test.info.EstimatedWork()
			if work < test.expectedMin || work > test.expectedMax {
				t.Errorf("EstimatedWork() = %d, expected between %d and %d",
					work, test.expectedMin, test.expectedMax)
			}
		})
	}
}

func TestInfo_FullName(t *testing.T) {
	info := &Info{
		Name:   "test_table",
		Schema: "public",
	}

	expected := "public.test_table"
	result := info.FullName()

	if result != expected {
		t.Errorf("FullName() = %q, expected %q", result, expected)
	}
}
