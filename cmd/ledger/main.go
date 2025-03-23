package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	DSN string
}

type Touchpoint struct {
	ID         int
	Program    string
	Scholar    string
	Staff      string
	Kind       string
	Channel    string
	OccurredAt time.Time
	Notes      string
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

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

	switch os.Args[1] {
	case "add":
		handleAdd(ctx, db, os.Args[2:])
	case "list":
		handleList(ctx, db, os.Args[2:])
	case "stats":
		handleStats(ctx, db, os.Args[2:])
	case "gaps":
		handleGaps(ctx, db, os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
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

func printUsage() {
	fmt.Println("groupscholar-touchpoint-ledger")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  ledger add --program <name> --scholar <name> --staff <name> --type <type> --channel <channel> [--date YYYY-MM-DD] [--notes text]")
	fmt.Println("  ledger list [--limit 20]")
	fmt.Println("  ledger stats [--days 30]")
	fmt.Println("  ledger gaps [--days 14]")
}

func handleAdd(ctx context.Context, db *sql.DB, args []string) {
	fs := flag.NewFlagSet("add", flag.ExitOnError)
	program := fs.String("program", "", "Program name")
	scholar := fs.String("scholar", "", "Scholar name")
	staff := fs.String("staff", "", "Staff name")
	kind := fs.String("type", "", "Touchpoint type")
	channel := fs.String("channel", "", "Channel")
	date := fs.String("date", "", "YYYY-MM-DD date")
	notes := fs.String("notes", "", "Notes")
	fs.Parse(args)

	if *program == "" || *scholar == "" || *staff == "" || *kind == "" || *channel == "" {
		fatal(errors.New("add requires --program, --scholar, --staff, --type, --channel"))
	}

	occurredAt := time.Now()
	if *date != "" {
		parsed, err := time.Parse("2006-01-02", *date)
		if err != nil {
			fatal(fmt.Errorf("invalid date: %w", err))
		}
		occurredAt = parsed
	}

	programID, err := lookupID(ctx, db, "programs", *program)
	if err != nil {
		fatal(err)
	}
	scholarID, err := lookupID(ctx, db, "scholars", *scholar)
	if err != nil {
		fatal(err)
	}
	staffID, err := lookupID(ctx, db, "staff", *staff)
	if err != nil {
		fatal(err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO gs_touchpoint_ledger.touchpoints
		(program_id, scholar_id, staff_id, kind, channel, occurred_at, notes)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, programID, scholarID, staffID, *kind, *channel, occurredAt, nullIfEmpty(*notes))
	if err != nil {
		fatal(err)
	}

	fmt.Println("Touchpoint logged.")
}

func handleList(ctx context.Context, db *sql.DB, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	limit := fs.Int("limit", 20, "Limit")
	fs.Parse(args)

	rows, err := db.QueryContext(ctx, `
		SELECT t.id, p.name, s.name, st.name, t.kind, t.channel, t.occurred_at, COALESCE(t.notes, '')
		FROM gs_touchpoint_ledger.touchpoints t
		JOIN gs_touchpoint_ledger.programs p ON p.id = t.program_id
		JOIN gs_touchpoint_ledger.scholars s ON s.id = t.scholar_id
		JOIN gs_touchpoint_ledger.staff st ON st.id = t.staff_id
		ORDER BY t.occurred_at DESC
		LIMIT $1
	`, *limit)
	if err != nil {
		fatal(err)
	}
	defer rows.Close()

	fmt.Printf("%-4s %-20s %-18s %-18s %-14s %-10s %-12s %s\n", "ID", "Program", "Scholar", "Staff", "Type", "Channel", "Date", "Notes")
	for rows.Next() {
		var tp Touchpoint
		if err := rows.Scan(&tp.ID, &tp.Program, &tp.Scholar, &tp.Staff, &tp.Kind, &tp.Channel, &tp.OccurredAt, &tp.Notes); err != nil {
			fatal(err)
		}
		fmt.Printf("%-4d %-20s %-18s %-18s %-14s %-10s %-12s %s\n", tp.ID, trim(tp.Program, 20), trim(tp.Scholar, 18), trim(tp.Staff, 18), trim(tp.Kind, 14), trim(tp.Channel, 10), tp.OccurredAt.Format("2006-01-02"), tp.Notes)
	}

	if err := rows.Err(); err != nil {
		fatal(err)
	}
}

func handleStats(ctx context.Context, db *sql.DB, args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	days := fs.Int("days", 30, "Days")
	fs.Parse(args)

	var total int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM gs_touchpoint_ledger.touchpoints
		WHERE occurred_at >= NOW() - ($1 || ' days')::interval
	`, *days).Scan(&total); err != nil {
		fatal(err)
	}

	fmt.Printf("Touchpoints in last %d days: %d\n", *days, total)

	rows, err := db.QueryContext(ctx, `
		SELECT channel, COUNT(*)
		FROM gs_touchpoint_ledger.touchpoints
		WHERE occurred_at >= NOW() - ($1 || ' days')::interval
		GROUP BY channel
		ORDER BY COUNT(*) DESC
	`, *days)
	if err != nil {
		fatal(err)
	}
	defer rows.Close()

	fmt.Println("By channel:")
	for rows.Next() {
		var channel string
		var count int
		if err := rows.Scan(&channel, &count); err != nil {
			fatal(err)
		}
		fmt.Printf("  %-10s %d\n", channel, count)
	}

	if err := rows.Err(); err != nil {
		fatal(err)
	}
}

func handleGaps(ctx context.Context, db *sql.DB, args []string) {
	fs := flag.NewFlagSet("gaps", flag.ExitOnError)
	days := fs.Int("days", 14, "Days")
	fs.Parse(args)

	rows, err := db.QueryContext(ctx, `
		SELECT s.name, s.cohort, p.name, MAX(t.occurred_at)
		FROM gs_touchpoint_ledger.scholars s
		JOIN gs_touchpoint_ledger.programs p ON p.id = s.program_id
		LEFT JOIN gs_touchpoint_ledger.touchpoints t ON t.scholar_id = s.id
		GROUP BY s.name, s.cohort, p.name
		HAVING MAX(t.occurred_at) IS NULL OR MAX(t.occurred_at) < NOW() - ($1 || ' days')::interval
		ORDER BY MAX(t.occurred_at) NULLS FIRST
	`, *days)
	if err != nil {
		fatal(err)
	}
	defer rows.Close()

	fmt.Printf("Scholars with no touchpoint in the last %d days:\n", *days)
	for rows.Next() {
		var name, cohort, program string
		var last sql.NullTime
		if err := rows.Scan(&name, &cohort, &program, &last); err != nil {
			fatal(err)
		}
		lastValue := "never"
		if last.Valid {
			lastValue = last.Time.Format("2006-01-02")
		}
		fmt.Printf("- %-18s %-10s %-18s last: %s\n", trim(name, 18), trim(cohort, 10), trim(program, 18), lastValue)
	}

	if err := rows.Err(); err != nil {
		fatal(err)
	}
}

func lookupID(ctx context.Context, db *sql.DB, table string, name string) (int, error) {
	var id int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT id FROM gs_touchpoint_ledger.%s WHERE name = $1", table), name).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("%s not found: %s", table, name)
	}
	return id, err
}

func trim(value string, max int) string {
	if len(value) <= max {
		return value
	}
	if max <= 1 {
		return value[:max]
	}
	return value[:max-1] + "."
}

func nullIfEmpty(value string) interface{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
