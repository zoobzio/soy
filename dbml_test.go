package cereal

import (
	"strings"
	"testing"

	"github.com/zoobzio/dbml"
	"github.com/zoobzio/sentinel"
)

type TestUser struct {
	ID        int64  `db:"id" type:"bigserial" constraints:"primary_key"`
	Email     string `db:"email" type:"text" constraints:"not_null,unique" index:"idx_user_email"`
	Name      string `db:"name" type:"text" constraints:"not_null"`
	Age       int    `db:"age" type:"integer" check:"age >= 18"`
	CreatedAt string `db:"created_at" type:"timestamptz" default:"now()"`
}

type TestProfile struct {
	ID     int64  `db:"id" type:"bigserial" constraints:"primary_key"`
	Bio    string `db:"bio" type:"text"`
	UserID *int64 `db:"user_id" type:"bigint" references:"users(id)"`
}

func TestBuildDBMLFromStruct(t *testing.T) {

	// Register tags with Sentinel
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")
	sentinel.Tag("check")
	sentinel.Tag("index")
	sentinel.Tag("references")
	sentinel.Tag("on_delete")
	sentinel.Tag("on_update")

	t.Run("basic table with constraints", func(t *testing.T) {
		// Inspect struct
		metadata := sentinel.Inspect[TestUser]()

		// Build DBML
		project, err := buildDBMLFromStruct(metadata, "users")
		if err != nil {
			t.Fatalf("buildDBMLFromStruct() error = %v", err)
		}

		// Validate project was created
		if project == nil {
			t.Fatal("buildDBMLFromStruct() returned nil project")
		}

		// Check table exists (keyed by schema.name)
		table, ok := project.Tables["public.users"]
		if !ok {
			t.Fatalf("users table not found in project. Available tables: %v", getTableKeys(project.Tables))
		}

		// Check table has correct number of columns
		if len(table.Columns) != 5 {
			t.Errorf("expected 5 columns, got %d", len(table.Columns))
		}

		// Verify column properties
		var idCol, emailCol, nameCol, ageCol, createdAtCol *struct {
			found bool
			col   interface{}
		}

		for _, col := range table.Columns {
			switch col.Name {
			case "id":
				idCol = &struct {
					found bool
					col   interface{}
				}{true, col}
				if col.Type != "bigserial" {
					t.Errorf("id column type = %s, want bigserial", col.Type)
				}
				if col.Settings == nil || !col.Settings.PrimaryKey {
					t.Error("id column should be primary key")
				}
			case "email":
				emailCol = &struct {
					found bool
					col   interface{}
				}{true, col}
				if col.Type != "text" {
					t.Errorf("email column type = %s, want text", col.Type)
				}
				if col.Settings == nil || !col.Settings.Unique {
					t.Error("email column should be unique")
				}
			case "name":
				nameCol = &struct {
					found bool
					col   interface{}
				}{true, col}
				if col.Type != "text" {
					t.Errorf("name column type = %s, want text", col.Type)
				}
			case "age":
				ageCol = &struct {
					found bool
					col   interface{}
				}{true, col}
				if col.Settings == nil || col.Settings.Check == nil {
					t.Error("age column should have check constraint")
				} else if *col.Settings.Check != "age >= 18" {
					t.Errorf("age check = %s, want 'age >= 18'", *col.Settings.Check)
				}
			case "created_at":
				createdAtCol = &struct {
					found bool
					col   interface{}
				}{true, col}
				if col.Settings == nil || col.Settings.Default == nil {
					t.Error("created_at column should have default")
				} else if *col.Settings.Default != "now()" {
					t.Errorf("created_at default = %s, want 'now()'", *col.Settings.Default)
				}
			}
		}

		if idCol == nil || !idCol.found {
			t.Error("id column not found")
		}
		if emailCol == nil || !emailCol.found {
			t.Error("email column not found")
		}
		if nameCol == nil || !nameCol.found {
			t.Error("name column not found")
		}
		if ageCol == nil || !ageCol.found {
			t.Error("age column not found")
		}
		if createdAtCol == nil || !createdAtCol.found {
			t.Error("created_at column not found")
		}

		// Check index exists
		if len(table.Indexes) != 1 {
			t.Errorf("expected 1 index, got %d", len(table.Indexes))
		} else {
			idx := table.Indexes[0]
			if idx.Name == nil || *idx.Name != "idx_user_email" {
				t.Errorf("index name = %v, want 'idx_user_email'", idx.Name)
			}
		}
	})

	t.Run("table with foreign key", func(t *testing.T) {
		// Inspect struct
		metadata := sentinel.Inspect[TestProfile]()

		// Build DBML
		project, err := buildDBMLFromStruct(metadata, "profiles")
		if err != nil {
			t.Fatalf("buildDBMLFromStruct() error = %v", err)
		}

		// Check table exists (keyed by schema.name)
		table, ok := project.Tables["public.profiles"]
		if !ok {
			t.Fatalf("profiles table not found in project. Available tables: %v", getTableKeys(project.Tables))
		}

		// Find user_id column
		var userIDCol *struct {
			found bool
		}
		for _, col := range table.Columns {
			if col.Name == "user_id" {
				userIDCol = &struct {
					found bool
				}{true}
				// Check inline ref exists
				if col.InlineRef == nil {
					t.Error("user_id should have inline reference")
				} else {
					if col.InlineRef.Table != "users" {
						t.Errorf("reference table = %s, want 'users'", col.InlineRef.Table)
					}
					if col.InlineRef.Column != "id" {
						t.Errorf("reference column = %s, want 'id'", col.InlineRef.Column)
					}
				}
				break
			}
		}

		if userIDCol == nil || !userIDCol.found {
			t.Error("user_id column not found")
		}
	})

	t.Run("generates valid DBML string", func(t *testing.T) {
		metadata := sentinel.Inspect[TestUser]()
		project, err := buildDBMLFromStruct(metadata, "users")
		if err != nil {
			t.Fatalf("buildDBMLFromStruct() error = %v", err)
		}

		// Generate DBML string
		dbmlString := project.Generate()

		// Check it contains expected elements
		if !strings.Contains(dbmlString, "Table users") {
			t.Error("DBML should contain 'Table users'")
		}
		if !strings.Contains(dbmlString, "id bigserial") {
			t.Error("DBML should contain 'id bigserial'")
		}
		if !strings.Contains(dbmlString, "email text") {
			t.Error("DBML should contain 'email text'")
		}
		if !strings.Contains(dbmlString, "[pk]") {
			t.Error("DBML should contain '[pk]'")
		}
		if !strings.Contains(dbmlString, "[unique") {
			t.Error("DBML should contain unique constraint")
		}
		if !strings.Contains(dbmlString, "idx_user_email") {
			t.Error("DBML should contain index name")
		}

		t.Logf("Generated DBML:\n%s", dbmlString)
	})
}

func getTableKeys(tables map[string]*dbml.Table) []string {
	keys := make([]string, 0, len(tables))
	for k := range tables {
		keys = append(keys, k)
	}
	return keys
}

func TestInferPostgresType(t *testing.T) {
	tests := []struct {
		goType   string
		expected string
	}{
		{"string", "TEXT"},
		{"int64", "BIGINT"},
		{"int", "INTEGER"},
		{"bool", "BOOLEAN"},
		{"time.Time", "TIMESTAMPTZ"},
		{"[]string", "TEXT[]"},
		{"[]int", "INTEGER[]"},
		{"[]byte", "BYTEA"},
		{"map[string]any", "JSONB"},
		{"*string", "TEXT"},
		{"*int64", "BIGINT"},
	}

	for _, tt := range tests {
		t.Run(tt.goType, func(t *testing.T) {
			result := inferPostgresType(tt.goType)
			if result != tt.expected {
				t.Errorf("inferPostgresType(%q) = %q, want %q", tt.goType, result, tt.expected)
			}
		})
	}
}

func TestParseReferenceTag(t *testing.T) {
	tests := []struct {
		name       string
		ref        string
		wantTable  string
		wantColumn string
		wantErr    bool
	}{
		{"valid reference", "users(id)", "users", "id", false},
		{"valid with underscores", "user_profiles(user_id)", "user_profiles", "user_id", false},
		{"missing parenthesis", "users", "", "", true},
		{"missing closing paren", "users(id", "", "", true},
		{"empty table", "(id)", "", "", true},
		{"empty column", "users()", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, column, err := parseReferenceTag(tt.ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseReferenceTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if table != tt.wantTable {
					t.Errorf("parseReferenceTag() table = %v, want %v", table, tt.wantTable)
				}
				if column != tt.wantColumn {
					t.Errorf("parseReferenceTag() column = %v, want %v", column, tt.wantColumn)
				}
			}
		})
	}
}
