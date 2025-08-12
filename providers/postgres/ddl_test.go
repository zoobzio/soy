package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/zoobzio/sentinel"
)

// Test types for DDL generation
type DDLNote struct {
	ID      string `db:"id" validate:"uuid" constraints:"pk"`
	Content string `db:"content"`
	Created string `db:"created_at" validate:"datetime"`
}

type DDLUser struct {
	ID       string                 `db:"id" validate:"uuid" constraints:"pk"`
	Email    string                 `db:"email" validate:"email" constraints:"unique,not_null"`
	IP       string                 `db:"last_ip" validate:"ip"`
	Location string                 `db:"location" validate:"json"`
	Tags     []string               `db:"tags"`
	Active   bool                   `db:"is_active" constraints:"not_null"`
	Score    float64                `db:"score"`
	Note     DDLNote                `db:"note_id" constraints:"fk"` // FK to notes table
	Details  DDLNote                `db:"details"`                  // No FK, should be JSONB
	Metadata map[string]interface{} `db:"metadata"`
}

func setupDDLTests() {
	// Register types in order (Note before User since User references Note)
	sentinel.Inspect[DDLNote]()
	sentinel.Inspect[DDLUser]()
}

func TestCreateTable(t *testing.T) {
	setupDDLTests()
	provider := &Provider{}

	t.Run("basic table with validate tags", func(t *testing.T) {
		metadata := sentinel.Inspect[DDLUser]()

		// Debug: print metadata
		t.Logf("Type name: %s", metadata.TypeName)
		t.Logf("Package: %s", metadata.PackageName)
		t.Logf("Metadata fields count: %d", len(metadata.Fields))
		for i, field := range metadata.Fields {
			t.Logf("Field %d: Name=%s, Type=%s, Tags=%v", i, field.Name, field.Type, field.Tags)
		}

		sql, err := provider.CreateTable(context.Background(), metadata, false)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		// Check table name
		if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS ddlusers") {
			t.Error("Expected table name 'ddlusers'")
		}

		// Check UUID type from validate tag
		if !strings.Contains(sql, "id UUID") {
			t.Error("Expected id to be UUID type from validate tag")
		}

		// Check INET type from validate tag
		if !strings.Contains(sql, "last_ip INET") {
			t.Error("Expected last_ip to be INET type from validate tag")
		}

		// Check JSONB type from validate tag
		if !strings.Contains(sql, "location JSONB") {
			t.Error("Expected location to be JSONB from validate tag")
		}

		// Check array type
		if !strings.Contains(sql, "tags TEXT[]") {
			t.Error("Expected tags to be TEXT[] array")
		}

		// Check embedded struct without FK becomes JSONB
		if !strings.Contains(sql, "details JSONB") {
			t.Error("Expected details field to be JSONB")
		}

		// Check constraints
		if !strings.Contains(sql, "email TEXT UNIQUE NOT NULL") {
			t.Error("Expected email to have UNIQUE NOT NULL constraints")
		}

		// Check primary key
		if !strings.Contains(sql, "PRIMARY KEY (id)") {
			t.Error("Expected PRIMARY KEY constraint on id")
		}

		// Check foreign key
		if !strings.Contains(sql, "FOREIGN KEY (note_id) REFERENCES ddlnotes(id)") {
			t.Error("Expected FOREIGN KEY constraint on note_id")
		}

		t.Log("Generated SQL:\n", sql)
	})

	t.Run("simple table", func(t *testing.T) {
		metadata := sentinel.Inspect[DDLNote]()
		sql, err := provider.CreateTable(context.Background(), metadata, false)
		if err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}

		if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS ddlnotes") {
			t.Error("Expected table name 'ddlnotes'")
		}

		if !strings.Contains(sql, "id UUID") {
			t.Error("Expected id to be UUID")
		}

		if !strings.Contains(sql, "PRIMARY KEY (id)") {
			t.Error("Expected PRIMARY KEY on id")
		}

		t.Log("Generated SQL:\n", sql)
	})
}
