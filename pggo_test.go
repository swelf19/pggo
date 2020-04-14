package main_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"git.u4b.ru/swelf/pggo/migrate"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/suite"
	"github.com/vaughan0/go-ini"
)

type PggoBinTestSuite struct {
	suite.Suite
	// m    *migrate.Migrator
	conn migrate.DBConnection
}

// Make sure that VariableThatShouldStartAtFive is set to five
// before each test
func (suite *PggoBinTestSuite) SetupTest() {

	var err error
	err = dropTables("t1", "t2", "t2", "schema_version")
	suite.Require().NoError(err, suite.T())

}

func (suite *PggoBinTestSuite) SetupSuite() {
	err := exec.Command("go", "build", "-o", "tmp/pggo").Run()
	// suite.Error(err)
	if err != nil {
		fmt.Println("Failed to build pggo binary:", err)
		suite.NoError(err)
		os.Exit(1)
	}
}

func dropTables(tables ...string) error {
	ctx := context.Background()
	connConfig, err := readConfig("testdata/pggo.conf")
	if err != nil {
		return err
	}

	// connConfig.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	// var err error = nil
	for _, t := range tables {
		code, err := conn.Exec(ctx, "drop table if exists "+t)
		if err != nil {
			fmt.Println(code)
			return err
		}
	}
	return err
}

func (suite *PggoBinTestSuite) TearDownTest() {
	// _, err := suite.conn.Exec(context.Background(), "ROLLBACK TO SAVEPOINT initial")
	// suite.Require().NoError(err, suite.T())
	// suite.conn.(pgx.Tx).Commit(context.Background())
	// suite.conn.(pgx.Tx).Conn().Close(context.Background())
}
func (suite *PggoBinTestSuite) TestInitWithoutDirectory() {
	defer func() {
		os.Remove("pggo.conf")
		os.Remove("001_create_people.sql.example")
	}()

	pggo(suite.T(), "init")

	expectedFiles := []string{"pggo.conf", "001_create_people.sql.example"}
	for _, f := range expectedFiles {
		_, err := os.Stat(f)
		suite.NoError(err)
	}

	// suite.Equal(migrate.MigrationNotFound{MigrationName: "migration_3"}, err)
}

func TestExampleTestSuite(t *testing.T) {

	suite.Run(t, new(PggoBinTestSuite))
}

// func TestMain(m *testing.M) {
// 	err := exec.Command("go", "build", "-o", "tmp/pggo").Run()
// 	if err != nil {
// 		fmt.Println("Failed to build pggo binary:", err)
// 		os.Exit(1)
// 	}

// 	os.Exit(m.Run())
// }

func readConfig(path string) (*pgx.ConnConfig, error) {
	file, err := ini.LoadFile(path)
	if err != nil {
		return nil, err
	}

	cp, _ := pgx.ParseConfig("")
	if s, ok := file.Get("database", "host"); ok {
		cp.Host = s
	}
	if p, ok := file.Get("database", "port"); ok {
		n, err := strconv.ParseUint(p, 10, 16)
		cp.Port = uint16(n)
		if err != nil {
			return nil, err
		}
	}

	if s, ok := file.Get("database", "database"); ok {
		cp.Database = s
	}
	if s, ok := file.Get("database", "user"); ok {
		cp.User = s
	}
	if s, ok := file.Get("database", "password"); ok {
		cp.Password = s
	}

	return cp, nil
}

func tableExists(t *testing.T, tableName string) bool {
	ctx := context.Background()
	connConfig, err := readConfig("testdata/pggo.conf")
	if err != nil {
		t.Fatal(err)
	}

	connConfig.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		connConfig.TLSConfig = nil
		conn, err = pgx.ConnectConfig(ctx, connConfig)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(
		ctx,
		"select exists(select 1 from information_schema.tables where table_catalog=$1 and table_name=$2)",
		connConfig.Database,
		tableName,
	).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func currentVersion(t *testing.T) int32 {
	output := pggo(t, "status", "-m", "testdata", "-c", "testdata/pggo.conf")
	re := regexp.MustCompile(`status:\s+(\d+)`)
	match := re.FindStringSubmatch(output)
	if match == nil {
		t.Fatalf("could not extract current version from status:\n%s", output)
	}

	n, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil {
		t.Fatal(err)
	}

	return int32(n)
}

func pggo(t *testing.T, args ...string) string {
	cmd := exec.Command("tmp/pggo", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("pggo failed with: %v\noutput:\n%v", err, string(output))
	}

	return string(output)
}

func (suite *PggoBinTestSuite) TestInitWithDirectory() {
	defer func() {
		os.RemoveAll("tmp/init")
	}()

	pggo(suite.T(), "init", "tmp/init")

	expectedFiles := []string{"tmp/init/pggo.conf", "tmp/init/001_create_people.sql.example"}
	for _, f := range expectedFiles {
		_, err := os.Stat(f)
		suite.NoError(err)
	}
}

func (suite *PggoBinTestSuite) TestNew() {
	path := "tmp/new"
	defer func() {
		os.RemoveAll(path)
	}()

	pggo(suite.T(), "init", path)
	pggo(suite.T(), "new", "-m", path, "first")
	pggo(suite.T(), "new", "-m", path, "second")

	expectedFiles := []string{"tmp/new/001_first.sql", "tmp/new/002_second.sql"}
	for _, f := range expectedFiles {
		_, err := os.Stat(f)
		suite.NoError(err)
	}
}

func (suite *PggoBinTestSuite) TestMigrate() {
	tests := []struct {
		args              []string
		expectedExists    []string
		expectedNotExists []string
	}{
		// {[]string{"-d", "0"}, []string{}, []string{"t1", "t2"}, 0},
		{[]string{}, []string{"t1", "t2"}, []string{}},
		{[]string{"001_create_t1.sql"}, []string{"t1"}, []string{"t2"}},
		{[]string{"002_create_t2.sql"}, []string{"t1", "t2"}, []string{}},
		{[]string{"001_create_t1.sql"}, []string{"t1"}, []string{"t2"}},
		// {[]string{"-d", "+1"}, []string{"t1", "t2"}, []string{}, 2},
		// {[]string{"-d", "-+1"}, []string{"t1", "t2"}, []string{}, 2},
	}

	for i, tt := range tests {
		// dropTables("t1", "t2", "t2", "schema_version")
		baseArgs := []string{"migrate", "-m", "testdata", "-c", "testdata/pggo.conf"}
		args := append(baseArgs, tt.args...)

		pggo(suite.T(), args...)

		for _, tableName := range tt.expectedExists {
			if !tableExists(suite.T(), tableName) {
				suite.T().Fatalf("%d. Expected table %s to exist, but it doesn't", i, tableName)
			}
		}

		for _, tableName := range tt.expectedNotExists {
			if tableExists(suite.T(), tableName) {
				suite.T().Fatalf("%d. Expected table %s to not exist, but it does", i, tableName)
			}
		}

		// if currentVersion(t) != tt.expectedVersion {
		// 	t.Fatalf(`Expected current version to be %d, but it was %d`, tt.expectedVersion, currentVersion(t))
		// }
	}
}

func migrationApplied(t *testing.T, migrationName string) bool {
	ctx := context.Background()
	connConfig, err := readConfig("testdata/pggo.conf")
	if err != nil {
		t.Fatal(err)
	}

	// connConfig.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		connConfig.TLSConfig = nil
		conn, err = pgx.ConnectConfig(ctx, connConfig)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(
		ctx,
		"select exists(select 1 from schema_version where migration_name=$1)",
		migrationName,
	).Scan(&exists)
	if err != nil {
		t.Fatal(err)
	}

	return exists
}

func (suite *PggoBinTestSuite) TestMigrateFake() {
	baseArgs := []string{"migrate", "-m", "testdata", "-c", "testdata/pggo.conf"}
	args := append(baseArgs, "001_create_t1.sql", "-f")
	output := pggo(suite.T(), args...)
	fmt.Println(output)
	suite.Equal(true, migrationApplied(suite.T(), "001_create_t1.sql"), "migration exists")
	suite.Equal(false, tableExists(suite.T(), "t1"), "table exists")

}

func (suite *PggoBinTestSuite) TestStatus() {
	// Ensure database is in clean state
	t := suite.T()
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf", "-d", "0")

	output := pggo(t, "status", "-m", "testdata", "-c", "testdata/pggo.conf")
	expected := `status:   migration(s) pending - 2`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}

	// Up all the way
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf")

	output = pggo(t, "status", "-m", "testdata", "-c", "testdata/pggo.conf")
	expected = `status:   up to date`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}

	// Back one
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf", "001_create_t1.sql")

	output = pggo(t, "status", "-m", "testdata", "-c", "testdata/pggo.conf")
	expected = `status:   migration(s) pending - 1`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}
}

func (suite *PggoBinTestSuite) TestCLIArgsWithoutConfigFile() {
	t := suite.T()
	// Ensure database is in clean state
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf")

	connConfig, err := readConfig("testdata/pggo.conf")
	if err != nil {
		t.Fatal(err)
	}

	output := pggo(t, "status",
		"-m", "testdata",
		"--host", connConfig.Host,
		"--port", strconv.FormatInt(int64(connConfig.Port), 10),
		"--user", connConfig.User,
		"--password", connConfig.Password,
		"--database", connConfig.Database,
	)
	expected := `status:   up to date`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}
}

func (suite *PggoBinTestSuite) TestConfigFileTemplateEvalWithEnvVar() {
	t := suite.T()
	// Ensure database is in clean state
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf")

	connConfig, err := readConfig("testdata/pggo.conf")
	if err != nil {
		t.Fatal(err)
	}

	err = os.Setenv("TERNHOST", connConfig.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.Unsetenv("TERNHOST")
		if err != nil {
			t.Fatal(err)
		}
	}()

	output := pggo(t, "status",
		"-c", "testdata/pggo-envvar.conf",
		"-m", "testdata",
		"--port", strconv.FormatInt(int64(connConfig.Port), 10),
		"--user", connConfig.User,
		"--password", connConfig.Password,
		"--database", connConfig.Database,
	)
	expected := `status:   up to date`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}
}

func (suite *PggoBinTestSuite) TestSSHTunnel() {
	t := suite.T()
	host := os.Getenv("TERN_HOST")
	if host == "" {
		t.Skip("Skipping SSH Tunnel test due to missing TERN_HOST environment variable")
	}

	user := os.Getenv("TERN_USER")
	if user == "" {
		t.Skip("Skipping SSH Tunnel test due to missing TERN_USER environment variable")
	}

	password := os.Getenv("TERN_PASSWORD")
	if password == "" {
		t.Skip("Skipping SSH Tunnel test due to missing TERN_PASSWORD environment variable")
	}

	database := os.Getenv("TERN_DATABASE")
	if database == "" {
		t.Skip("Skipping SSH Tunnel test due to missing TERN_DATABASE environment variable")
	}

	// Ensure database is in clean state
	pggo(t, "migrate", "-m", "testdata", "-c", "testdata/pggo.conf", "-d", "0")

	output := pggo(t, "status",
		"-m", "testdata",
		"--ssh-host", "localhost",
		"--host", host,
		"--user", user,
		"--password", password,
		"--database", database,
	)
	expected := `status:   migration(s) pending
version:  0 of 2`
	if !strings.Contains(output, expected) {
		t.Errorf("Expected status output to contain `%s`, but it didn't. Output:\n%s", expected, output)
	}
}
