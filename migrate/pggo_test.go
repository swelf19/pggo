package migrate_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/swelf19/pggo/v2/migrate"
)

// func TestMain(m *testing.M) {
// 	err := exec.Command("go", "build", "-o", "tmp/pggo").Run()
// 	if err != nil {
// 		fmt.Println("Failed to build pggo binary:", err)
// 		os.Exit(1)
// 	}

// 	os.Exit(m.Run())
// }

// var conn DBConnection

// func BTestNew(t *testing.T) {
// 	var err error
// 	conn, err = pgx.Connect(context.Background(), os.Getenv("MIGRATE_TEST_CONN_STRING"))
// 	if err != nil {
// 		t.Fatalf("error - %v", err)
// 	}

// 	m, err := migrate.NewMigrator(context.Background(), conn, "schema_version")
// 	err = m.LoadMigrations("/home/swelf/src/pggo/sample/")
// 	// fmt.Println(err)
// 	fmt.Println(m.MigrationsToApply(context.Background()))
// 	err = m.MigrateTo(context.Background(), "")
// 	fmt.Println(err)

// }

type MigrateTestSuite struct {
	suite.Suite
	m    *migrate.Migrator
	conn migrate.DBConnection
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *MigrateTestSuite) SetupTest() {

	var err error

	os.Setenv("MIGRATE_TEST_CONN_STRING", "host=127.0.0.1 database=tern_migrate_test user=postgres password=12345")
	conn, err := pgx.Connect(context.Background(), os.Getenv("MIGRATE_TEST_CONN_STRING"))
	suite.Require().NoError(err, suite.T())

	tx, err := conn.Begin(context.Background())
	suite.Require().NoError(err, suite.T())

	suite.conn = tx

	suite.m, err = migrate.NewMigrator(context.Background(), suite.conn, "schema_version")
	suite.Require().NoError(err, suite.T())

	//clearing schema migration table
	_, err = suite.conn.Exec(context.Background(), "delete from "+"schema_version")
	suite.Require().NoError(err, suite.T())

	_, err = suite.conn.Exec(context.Background(), "SAVEPOINT initial")
	suite.Require().NoError(err, suite.T())
}

func (suite *MigrateTestSuite) TearDownTest() {
	_, err := suite.conn.Exec(context.Background(), "ROLLBACK TO SAVEPOINT initial")
	suite.Require().NoError(err, suite.T())
	suite.conn.(pgx.Tx).Commit(context.Background())
	suite.conn.(pgx.Tx).Conn().Close(context.Background())
}

// All methods that begin with "Test" are run as tests within a
// suite.

func (suite *MigrateTestSuite) TestFullMigrations() {
	err := suite.m.LoadMigrations("testdata/sample/")
	suite.Require().NoError(err, suite.T())
	suite.Equal(3, len(suite.m.Migrations))

	migration, err := suite.m.MigrationsToApply(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(3, len(migration))

	currentMigrations, err := suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(0, len(currentMigrations))

	err = suite.m.Migrate(context.Background())
	suite.Require().NoError(err, suite.T())

	//Repeat full migtaion
	err = suite.m.Migrate(context.Background())
	suite.Require().NoError(err, suite.T())

	currentMigrations, err = suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(3, len(currentMigrations))
}

func (suite *MigrateTestSuite) TestPartialMigrations() {
	suite.m.Migrations = make(map[string]*migrate.Migration)
	err := suite.m.LoadMigrations("testdata/sample/")
	firstMigrationName := "001_create_t1.sql"

	err = suite.m.MigrateTo(context.Background(), firstMigrationName)
	currentMigrations, err := suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(1, len(currentMigrations))
	suite.Equal(currentMigrations[0], firstMigrationName)

	needToApply, err := suite.m.MigrationsToApply(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(len(needToApply), 2)

	secondMigrationName := "002_create_t2.sql"
	err = suite.m.MigrateTo(context.Background(), secondMigrationName)
	currentMigrations, err = suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal(2, len(currentMigrations))
	suite.Equal(currentMigrations[1], secondMigrationName)
}

func (suite *MigrateTestSuite) TestDetectDirection() {
	suite.m.Migrations = make(map[string]*migrate.Migration)
	suite.m.AppendMigration("migration_1", "create table t1(id serial primary key);", "drop table if exists t1;")
	suite.m.AppendMigration("migration_2", "create table t2(id serial primary key);", "drop table if exists t2;")
	suite.m.AppendMigration("migration_3", "create table t3(id serial primary key);", "drop table if exists t3;")

	err := suite.m.MigrateTo(context.Background(), "migration_2")
	suite.Require().NoError(err, suite.T())

	direction, err := suite.m.GetDirection(context.Background(), "migration_2")
	suite.Require().NoError(err, suite.T())
	suite.Equal(migrate.Back, direction)

	direction, err = suite.m.GetDirection(context.Background(), "migration_3")
	suite.Require().NoError(err, suite.T())
	suite.Equal(migrate.Forward, direction)

	direction, err = suite.m.GetDirection(context.Background(), "migration_4")
	suite.Require().NoError(err, suite.T())
	suite.Equal(migrate.NotFound, direction)

}

func (suite *MigrateTestSuite) isTableExists(tableName string) bool {
	var v bool
	query := fmt.Sprintf(`SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_name = $1
		)`)
	err := suite.conn.QueryRow(context.Background(), query, tableName).Scan(&v)
	if err != nil {
		return false
	}
	return v
}

func (suite *MigrateTestSuite) TestForwardBackMigration() {
	suite.m.Migrations = make(map[string]*migrate.Migration)
	suite.m.AppendMigration("migration_1", "create table t1(id serial primary key);", "drop table if exists t1;")
	suite.m.AppendMigration("migration_2", "create table t2(id serial primary key);", "drop table if exists t2;")
	suite.m.AppendMigration("migration_3", "create table t3(id serial primary key);", "drop table if exists t3;")

	err := suite.m.MigrateTo(context.Background(), "migration_3")
	suite.Require().NoError(err, suite.T())
	currentMigrations, err := suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal([]string{"migration_1", "migration_2", "migration_3"}, currentMigrations)
	suite.Equal(true, suite.isTableExists("t1"), "t1 exists")
	suite.Equal(true, suite.isTableExists("t2"), "t2 exists")
	suite.Equal(true, suite.isTableExists("t3"), "t3 exists")

	err = suite.m.MigrateTo(context.Background(), "migration_1")
	suite.Require().NoError(err, suite.T())
	currentMigrations, err = suite.m.GetCurrentVersion(context.Background())
	suite.Require().NoError(err, suite.T())
	suite.Equal([]string{"migration_1"}, currentMigrations)
	suite.Equal(true, suite.isTableExists("t1"), "t1 exists")
	suite.Equal(false, suite.isTableExists("t2"), "t2 exists")
	suite.Equal(false, suite.isTableExists("t3"), "t3 exists")
}
func (suite *MigrateTestSuite) TestSchemaVersionInitialization() {
	var err error
	_, err = suite.conn.Exec(context.Background(), "drop table if exists "+"schema_version")
	// fmt.Println(a)
	suite.Require().NoError(err, suite.T())
	_, err = migrate.NewMigrator(context.Background(), suite.conn, "schema_version")
	suite.NoError(err)
}

func (suite *MigrateTestSuite) TestWrongMigration() {
	suite.m.Migrations = make(map[string]*migrate.Migration)
	err := suite.m.MigrateTo(context.Background(), "migration_3")
	suite.Equal(migrate.MigrationNotFound{MigrationName: "migration_3"}, err)
}

func (suite *MigrateTestSuite) TestNoMigrations() {
	suite.m.Migrations = make(map[string]*migrate.Migration)
	err := suite.m.Migrate(context.Background())
	suite.NoError(err)

	// suite.Equal(migrate.MigrationNotFound{MigrationName: "migration_3"}, err)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestExampleTestSuite(t *testing.T) {

	suite.Run(t, new(MigrateTestSuite))
}

func TestHelpers(t *testing.T) {
	strings := []string{"test1", "test2", "test3"}
	assert.Equal(t, 0, migrate.Position(strings, "test1"))
	assert.Equal(t, 2, migrate.Position(strings, "test3"))
	assert.Equal(t, -1, migrate.Position(strings, "test4"))

	a := []string{"test1", "test2", "test3"}
	migrate.Reverse(a)
	assert.Equal(t, []string{"test3", "test2", "test1"}, a)

	a = []string{"test1", "test2"}
	migrate.Reverse(a)
	assert.Equal(t, []string{"test2", "test1"}, a)

	a = []string{"test1"}
	migrate.Reverse(a)
	assert.Equal(t, []string{"test1"}, a)

	a = []string{}
	migrate.Reverse(a)
	assert.Equal(t, []string{}, a)

}
