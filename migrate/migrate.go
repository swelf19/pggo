package migrate

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type MigraionDirection int

const (
	Back MigraionDirection = iota
	Forward
	NotFound
)

type DBConnection interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

var ErrNoMigrations = errors.Errorf("no rows in result set")

var migrationPattern = regexp.MustCompile(`\A(\d+)_.+\.sql\z`)

var ErrNoFwMigration = errors.Errorf("no sql in forward migration step")

type BadVersionError string

func (e BadVersionError) Error() string {
	return string(e)
}

type IrreversibleMigrationError struct {
	m *Migration
}

func (e IrreversibleMigrationError) Error() string {
	return fmt.Sprintf("Irreversible migration: %d - %s", e.m.Sequence, e.m.Name)
}

type MigrationNotFound struct {
	MigrationName string
}

func (e MigrationNotFound) Error() string {
	return fmt.Sprintf(`Migration "%s" not found`, e.MigrationName)
}

type NoMigrationsFoundError struct {
	Path string
}

func (e NoMigrationsFoundError) Error() string {
	return fmt.Sprintf("No migrations found at %s", e.Path)
}

type MigrationPgError struct {
	Sql string
	*pgconn.PgError
}

type Migration struct {
	Sequence int32
	Name     string
	UpSQL    string
	DownSQL  string
}

type MigratorOptions struct {
	// DisableTx causes the Migrator not to run migrations in a transaction.
	DisableTx bool
	// MigratorFS is the interface used for collecting the migrations.
	MigratorFS MigratorFS
}

type Migrator struct {
	conn         DBConnection
	versionTable string
	options      *MigratorOptions
	Migrations   map[string]*Migration
	OnStart      func(int32, string, string, string) // OnStart is called when a migration is run with the sequence, name, direction, and SQL
	Data         map[string]interface{}              // Data available to use in migrations
}

// NewMigrator initializes a new Migrator. It is highly recommended that versionTable be schema qualified.
func NewMigrator(ctx context.Context, conn DBConnection, versionTable string) (m *Migrator, err error) {
	return NewMigratorEx(ctx, conn, versionTable, &MigratorOptions{MigratorFS: defaultMigratorFS{}})
}

// NewMigratorEx initializes a new Migrator. It is highly recommended that versionTable be schema qualified.
func NewMigratorEx(ctx context.Context, conn DBConnection, versionTable string, opts *MigratorOptions) (m *Migrator, err error) {
	m = &Migrator{conn: conn, versionTable: versionTable, options: opts}
	err = m.ensureSchemaVersionTableExists(ctx)
	m.Migrations = make(map[string]*Migration)
	m.Data = make(map[string]interface{})
	return
}

type MigratorFS interface {
	ReadDir(dirname string) ([]os.FileInfo, error)
	ReadFile(filename string) ([]byte, error)
	Glob(Pattern string) (matches []string, err error)
}

type defaultMigratorFS struct{}

func (defaultMigratorFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	return ioutil.ReadDir(dirname)
}

func (defaultMigratorFS) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

func (defaultMigratorFS) Glob(Pattern string) ([]string, error) {
	return filepath.Glob(Pattern)
}

func FindMigrationsEx(path string, fs MigratorFS) ([]string, error) {
	path = strings.TrimRight(path, string(filepath.Separator))

	fileInfos, err := fs.ReadDir(path)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(fileInfos))
	for _, fi := range fileInfos {
		if fi.IsDir() {
			continue
		}

		matches := migrationPattern.FindStringSubmatch(fi.Name())
		if len(matches) != 2 {
			continue
		}

		// n, err := strconv.ParseInt(matches[1], 10, 32)
		// if err != nil {
		// 	// The regexp already validated that the prefix is all digits so this *should* never fail
		// 	return nil, err
		// }

		// if n < int64(len(paths)+1) {
		// 	return nil, fmt.Errorf("Duplicate migration %d", n)
		// }

		// if int64(len(paths)+1) < n {
		// 	return nil, fmt.Errorf("Missing migration %d", len(paths)+1)
		// }

		paths = append(paths, filepath.Join(path, fi.Name()))
	}

	return paths, nil
}

func FindMigrations(path string) ([]string, error) {
	return FindMigrationsEx(path, defaultMigratorFS{})
}

func (m *Migrator) LoadMigrations(path string) error {
	path = strings.TrimRight(path, string(filepath.Separator))
	mainTmpl := template.New("main")
	sharedPaths, err := m.options.MigratorFS.Glob(filepath.Join(path, "*", "*.sql"))
	if err != nil {
		return err
	}

	for _, p := range sharedPaths {
		body, err := m.options.MigratorFS.ReadFile(p)
		if err != nil {
			return err
		}

		name := strings.Replace(p, path+string(filepath.Separator), "", 1)
		_, err = mainTmpl.New(name).Parse(string(body))
		if err != nil {
			return err
		}
	}

	paths, err := FindMigrationsEx(path, m.options.MigratorFS)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return NoMigrationsFoundError{Path: path}
	}

	for _, p := range paths {
		body, err := m.options.MigratorFS.ReadFile(p)
		if err != nil {
			return err
		}

		pieces := strings.SplitN(string(body), "---- create above / drop below ----", 2)
		var upSQL, downSQL string
		upSQL = strings.TrimSpace(pieces[0])
		upSQL, err = m.evalMigration(mainTmpl.New(filepath.Base(p)+" up"), upSQL)
		if err != nil {
			return err
		}
		// Make sure there is SQL in the forward migration step.
		containsSQL := false
		for _, v := range strings.Split(upSQL, "\n") {
			// Only account for regular single line comment, empty line and space/comment combination
			cleanString := strings.TrimSpace(v)
			if len(cleanString) != 0 &&
				!strings.HasPrefix(cleanString, "--") {
				containsSQL = true
				break
			}
		}
		if !containsSQL {
			return ErrNoFwMigration
		}

		if len(pieces) == 2 {
			downSQL = strings.TrimSpace(pieces[1])
			downSQL, err = m.evalMigration(mainTmpl.New(filepath.Base(p)+" down"), downSQL)
			if err != nil {
				return err
			}
		}

		m.AppendMigration(filepath.Base(p), upSQL, downSQL)
	}

	return nil
}

func (m *Migrator) evalMigration(tmpl *template.Template, sql string) (string, error) {
	tmpl, err := tmpl.Parse(sql)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, m.Data)
	if err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (m *Migrator) AppendMigration(name, upSQL, downSQL string) {
	m.Migrations[name] = &Migration{
		Sequence: int32(len(m.Migrations)) + 1,
		Name:     name,
		UpSQL:    upSQL,
		DownSQL:  downSQL,
	}
	return
}

// Migrate runs pending migrations
// It calls m.OnStart when it begins a migration
func (m *Migrator) Migrate(ctx context.Context) error {
	// return m.MigrateTo(ctx, int32(len(m.Migrations)))
	migrations, err := m.MigrationsToApply(ctx)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return nil
	}
	return m.MigrateTo(ctx, migrations[len(migrations)-1])
}

// Lock to ensure multiple migrations cannot occur simultaneously
const lockNum = int64(9628173550095224) // arbitrary random number

func (m *Migrator) acquireAdvisoryLock(ctx context.Context) error {
	_, err := m.conn.Exec(ctx, "select pg_advisory_lock($1)", lockNum)
	return err
}

func (m *Migrator) releaseAdvisoryLock(ctx context.Context) error {
	_, err := m.conn.Exec(ctx, "select pg_advisory_unlock($1)", lockNum)
	return err
}

//ApplyMigration applies migration
// func (m *Migrator) ApplyMigration(ctx context.Context, migration string) (err error) {

// }

func (m *Migrator) MigrationsToApply(ctx context.Context) ([]string, error) {
	currentMigrations, err := m.GetCurrentVersion(ctx)
	// fmt.Println(err)
	if err != nil && err != ErrNoMigrations {
		return []string{}, err
	}
	// fmt.Println(m.Migrations)
	var found bool
	toApply := []string{}
	for _, migrationCandidate := range m.Migrations {
		found = false
		for _, migrationApplied := range currentMigrations {
			if migrationCandidate.Name == migrationApplied {
				found = true
				break
			}
		}
		if !found {
			toApply = append(toApply, migrationCandidate.Name)
		}
	}
	sort.Strings(toApply)
	return toApply, nil
}

func Position(a []string, x string) int {
	for i, s := range a {
		if x == s {
			return i
		}
	}
	return -1
}

func Reverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func (m *Migrator) GetDirection(ctx context.Context, targetMigration string) (MigraionDirection, error) {
	currentMigrations, err := m.GetCurrentVersion(ctx)

	if err != nil && err != ErrNoMigrations {
		return Back, err
	}
	_, foundNew := m.Migrations[targetMigration]
	if Position(currentMigrations, targetMigration) >= 0 {
		return Back, nil
	} else if foundNew {
		return Forward, nil
	}
	return NotFound, nil
}

// MigrateTo migrates to targetVersion
func (m *Migrator) MigrateTo(ctx context.Context, targetMigration string) (err error) {
	err = m.acquireAdvisoryLock(ctx)
	if err != nil {
		return err
	}
	defer func() {
		unlockErr := m.releaseAdvisoryLock(ctx)
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()

	// currentMigrations, err := m.GetCurrentVersion(ctx)
	// if err != nil && err != ErrNoMigrations {
	// 	return err
	// }
	// fmt.Println(currentMigrations)

	// var direction int32
	// if currentMigrations < targetMigration {
	// 	direction = 1
	// } else {
	// 	direction = -1
	// }
	direction, err := m.GetDirection(ctx, targetMigration)
	if err != nil {
		return err
	}
	// fmt.Println(direction)
	var migrationsToApply []string
	if direction == Forward {
		migrationsToApply, err = m.MigrationsToApply(ctx)
		if err != nil {
			return err
		}
	} else if direction == Back {
		currentMigrations, err := m.GetCurrentVersion(ctx)
		if err != nil {
			return err
		}
		Reverse(currentMigrations)
		migrationsToApply = currentMigrations[:Position(currentMigrations, targetMigration)]
	} else if direction == NotFound {
		return MigrationNotFound{MigrationName: targetMigration}
	}

	for _, currentName := range migrationsToApply {
		var current *Migration
		var sql, directionName string
		// var sequence int32
		// if direction == 1 {
		current = m.Migrations[currentName]
		// sequence = current.Sequence
		if direction == Forward {
			sql = current.UpSQL
		} else {
			sql = current.DownSQL
		}
		// directionName = "up"
		// } else {
		// 	current = m.Migrations[currentMigrations-1]
		// 	sequence = current.Sequence - 1
		// 	sql = current.DownSQL
		// 	directionName = "down"
		// 	if current.DownSQL == "" {
		// 		return IrreversibleMigrationError{m: current}
		// 	}
		// }

		var tx pgx.Tx
		if !m.options.DisableTx {
			tx, err = m.conn.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
		}

		// Fire on start callback
		if m.OnStart != nil {
			m.OnStart(current.Sequence, current.Name, directionName, sql)
		}
		if sql != "" {
			// Execute the migration
			_, err = m.conn.Exec(ctx, sql)
			if err != nil {
				// fmt.Println(current.Name)
				if err, ok := err.(*pgconn.PgError); ok {
					return MigrationPgError{Sql: sql, PgError: err}
				}
				return err
			}
		}

		// Reset all database connection settings. Important to do before updating version as search_path may have been changed.
		m.conn.Exec(ctx, "reset all")

		// Add one to the version
		// err = m.markMigrationApplied(ctx, current.Name)
		if direction == Forward {
			err = m.markMigrationApplied(ctx, current.Name)
		} else {
			err = m.markMigrationUnapplied(ctx, current.Name)
		}
		if err != nil {
			return err
		}
		if !m.options.DisableTx {
			err = tx.Commit(ctx)
			if err != nil {
				return err
			}
		}

		if targetMigration == currentName {
			//we have done with migrations, exiting
			return nil
		}

		// currentMigrations = currentMigrations + direction
	}

	return nil
}

func (m *Migrator) markMigrationApplied(ctx context.Context, migrationsName string) error {
	query := fmt.Sprintf("insert into %s (migration_name,migrated_at) values ($1,now())", m.versionTable)
	_, err := m.conn.Exec(ctx,
		query,
		migrationsName,
	)
	if err != nil {
		return err
	}
	return nil
}
func (m *Migrator) markMigrationUnapplied(ctx context.Context, migrationsName string) error {
	query := fmt.Sprintf("delete from %s where migration_name=$1", m.versionTable)
	_, err := m.conn.Exec(ctx,
		query,
		migrationsName,
	)
	if err != nil {
		return err
	}
	return nil
}

func (m *Migrator) GetCurrentVersion(ctx context.Context) (v []string, err error) {
	migrations := make([]string, 0)
	rows, err := m.conn.Query(ctx,
		fmt.Sprintf("select migration_name from %s order by migrated_at",
			m.versionTable),
	)
	defer rows.Close()
	if err == ErrNoMigrations {
		return migrations, nil
	}
	for rows.Next() {
		var v string
		rows.Scan(&v)
		migrations = append(migrations, v)
	}
	return migrations, err
}

func (m *Migrator) ensureSchemaVersionTableExists(ctx context.Context) (err error) {
	err = m.acquireAdvisoryLock(ctx)
	if err != nil {
		return err
	}
	defer func() {
		unlockErr := m.releaseAdvisoryLock(ctx)
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()
	exists := m.isMigrationTableExists(ctx)
	if exists == true {
		return nil
	}

	// if pgErr, ok := err.(*pgconn.PgError); !ok || pgErr.Code != pgerrcode.UndefinedTable {
	// 	return err
	// }

	err = m.createMigrationTable(ctx)

	return err

}

func (m *Migrator) isMigrationTableExists(ctx context.Context) bool {
	var v bool
	var err error
	query := fmt.Sprintf(`SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_name = $1
		)`)
	err = m.conn.QueryRow(ctx, query, m.versionTable).Scan(&v)
	if err != nil {
		v = false
	}
	return v
}

func (m *Migrator) createMigrationTable(ctx context.Context) (err error) {
	_, err = m.conn.Exec(ctx, fmt.Sprintf(`
	create table if not exists %s(
		id serial primary key,
		migration_name character varying(255) not null, 
		migrated_at timestamp with time zone)
	 `, m.versionTable))
	return err
}
