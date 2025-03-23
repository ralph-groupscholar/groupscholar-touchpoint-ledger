package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fatal(err)
	}

	ctx := context.Background()

	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fatal(err)
	}

	if err := migrate(ctx, db); err != nil {
		fatal(err)
	}
	if err := seed(ctx, db); err != nil {
		fatal(err)
	}

	fmt.Println("Schema migrated and seeded.")
}

type Config struct {
	DSN string
}

func loadConfig() (Config, error) {
	dsn := strings.TrimSpace(os.Getenv("GS_TOUCHPOINT_DSN"))
	if dsn == "" {
		dsn = buildDSNFromEnv()
	}
	if dsn == "" {
		return Config{}, errors.New("missing GS_TOUCHPOINT_DSN or PG* environment variables")
	}
	return Config{DSN: dsn}, nil
}

func buildDSNFromEnv() string {
	host := os.Getenv("PGHOST")
	port := os.Getenv("PGPORT")
	user := os.Getenv("PGUSER")
	password := os.Getenv("PGPASSWORD")
	dbname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if host == "" || user == "" || dbname == "" {
		return ""
	}

	if port == "" {
		port = "5432"
	}
	if sslmode == "" {
		sslmode = "require"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, password, host, port, dbname, sslmode)
}

func migrate(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS gs_touchpoint_ledger`,
		`CREATE TABLE IF NOT EXISTS gs_touchpoint_ledger.programs (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			region TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS gs_touchpoint_ledger.staff (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			role TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS gs_touchpoint_ledger.scholars (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			cohort TEXT NOT NULL,
			program_id INT NOT NULL REFERENCES gs_touchpoint_ledger.programs(id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS gs_touchpoint_ledger.touchpoints (
			id SERIAL PRIMARY KEY,
			program_id INT NOT NULL REFERENCES gs_touchpoint_ledger.programs(id),
			scholar_id INT NOT NULL REFERENCES gs_touchpoint_ledger.scholars(id),
			staff_id INT NOT NULL REFERENCES gs_touchpoint_ledger.staff(id),
			kind TEXT NOT NULL,
			channel TEXT NOT NULL,
			occurred_at TIMESTAMPTZ NOT NULL,
			notes TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_touchpoints_occurred_at ON gs_touchpoint_ledger.touchpoints(occurred_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_touchpoints_scholar ON gs_touchpoint_ledger.touchpoints(scholar_id)`,
	}

	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func seed(ctx context.Context, db *sql.DB) error {
	programs := []struct {
		Name   string
		Region string
	}{
		{"Future Scholars North", "Midwest"},
		{"STEM Horizon", "South"},
		{"Bridge to Campus", "West"},
	}

	for _, program := range programs {
		_, err := db.ExecContext(ctx, `
			INSERT INTO gs_touchpoint_ledger.programs (name, region)
			VALUES ($1, $2)
			ON CONFLICT (name) DO UPDATE SET region = EXCLUDED.region
		`, program.Name, program.Region)
		if err != nil {
			return err
		}
	}

	staff := []struct {
		Name string
		Role string
	}{
		{"Daria Mendez", "Scholar Success"},
		{"Jordan Lee", "Program Director"},
		{"Imani Patel", "Mentor Liaison"},
	}

	for _, person := range staff {
		_, err := db.ExecContext(ctx, `
			INSERT INTO gs_touchpoint_ledger.staff (name, role)
			VALUES ($1, $2)
			ON CONFLICT (name) DO UPDATE SET role = EXCLUDED.role
		`, person.Name, person.Role)
		if err != nil {
			return err
		}
	}

	scholars := []struct {
		Name    string
		Cohort  string
		Program string
	}{
		{"Avery Green", "2026", "Future Scholars North"},
		{"Nico Alvarez", "2025", "STEM Horizon"},
		{"Priya Shah", "2027", "Bridge to Campus"},
		{"Mateo Cruz", "2026", "Future Scholars North"},
		{"Jules Martin", "2025", "STEM Horizon"},
	}

	for _, scholar := range scholars {
		programID, err := lookupID(ctx, db, "programs", scholar.Program)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, `
			INSERT INTO gs_touchpoint_ledger.scholars (name, cohort, program_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (name) DO UPDATE SET cohort = EXCLUDED.cohort, program_id = EXCLUDED.program_id
		`, scholar.Name, scholar.Cohort, programID)
		if err != nil {
			return err
		}
	}

	seedTouchpoints := []struct {
		Program  string
		Scholar  string
		Staff    string
		Kind     string
		Channel  string
		DaysAgo  int
		Notes    string
	}{
		{"Future Scholars North", "Avery Green", "Daria Mendez", "Check-in", "Call", 4, "Reviewed midterm goals and internship applications."},
		{"STEM Horizon", "Nico Alvarez", "Jordan Lee", "Mentor Match", "Email", 12, "Shared three mentor matches for spring semester."},
		{"Bridge to Campus", "Priya Shah", "Imani Patel", "Workshop", "Zoom", 20, "Attended financial aid workshop."},
		{"Future Scholars North", "Mateo Cruz", "Daria Mendez", "Campus Visit", "In-person", 30, "Visited campus and met admissions."},
		{"STEM Horizon", "Jules Martin", "Imani Patel", "Check-in", "SMS", 7, "Confirmed scholarship application timeline."},
	}

	for _, tp := range seedTouchpoints {
		programID, err := lookupID(ctx, db, "programs", tp.Program)
		if err != nil {
			return err
		}
		scholarID, err := lookupID(ctx, db, "scholars", tp.Scholar)
		if err != nil {
			return err
		}
		staffID, err := lookupID(ctx, db, "staff", tp.Staff)
		if err != nil {
			return err
		}
		occurredAt := time.Now().AddDate(0, 0, -tp.DaysAgo)
		_, err = db.ExecContext(ctx, `
			INSERT INTO gs_touchpoint_ledger.touchpoints
			(program_id, scholar_id, staff_id, kind, channel, occurred_at, notes)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, programID, scholarID, staffID, tp.Kind, tp.Channel, occurredAt, tp.Notes)
		if err != nil {
			return err
		}
	}

	return nil
}

func lookupID(ctx context.Context, db *sql.DB, table string, name string) (int, error) {
	var id int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT id FROM gs_touchpoint_ledger.%s WHERE name = $1", table), name).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("%s not found: %s", table, name)
	}
	return id, err
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
