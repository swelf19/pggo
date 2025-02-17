package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
	"github.com/swelf19/pggo/v2/migrate"
	ini "github.com/vaughan0/go-ini"
)

const VERSION = "2.0.3"

var defaultConf = `[database]
# host is required (network host or path to Unix domain socket)
# host =
# port = 5432
# database is required
# database =
# user defaults to OS user
# user =
# password =
# version_table = public.schema_version
#
# sslmode generally matches the behavior described in:
# http://www.postgresql.org/docs/9.4/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION
#
# There are only two modes that most users should use:
# prefer - on trusted networks where security is not required
# verify-full - require SSL connection
# sslmode = prefer
#
# sslrootcert is generally used with sslmode=verify-full
# sslrootcert = /path/to/root/ca

# Proxy the above database connection via SSH
# [ssh-tunnel]
# host =
# port = 22
# user defaults to OS user
# user =
# password is not required if using SSH agent authentication
# password =

[data]
# Any fields in the data section are available in migration templates
# prefix = foo
`

var sampleMigration = `-- This is a sample migration.

create table people(
  id serial primary key,
  first_name varchar not null,
  last_name varchar not null
);

---- create above / drop below ----

drop table people;
`

var newMigrationText = `-- Write your migrate up statements here

---- create above / drop below ----

-- Write your migrate down statements here. If this migration is irreversible
-- Then delete the separator line above.
`

type Config struct {
	ConnConfig    pgx.ConnConfig
	SslMode       string
	SslRootCert   string
	VersionTable  string
	Data          map[string]interface{}
	SSHConnConfig SSHConnConfig
}

var cliOptions struct {
	destinationVersion string
	migrationsPath     string
	configPath         string

	host          string
	port          uint16
	user          string
	password      string
	database      string
	sslmode       string
	sslrootcert   string
	versionTable  string
	fakeMigration bool

	sshHost     string
	sshPort     string
	sshUser     string
	sshPassword string
}

func (c *Config) Validate() error {
	if c.ConnConfig.Host == "" {
		return errors.New("Config must contain host but it does not")
	}

	if c.ConnConfig.Database == "" {
		return errors.New("Config must contain database but it does not")
	}

	switch c.SslMode {
	case "", "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		// okay
	default:
		return errors.New("sslmode is invalid")
	}

	return nil
}

// migrate.DBConnection
func (c *Config) Connect(ctx context.Context) (*pgx.Conn, error) {
	if c.SSHConnConfig.Host != "" {
		client, err := NewSSHClient(&c.SSHConnConfig)
		if err != nil {
			return nil, err
		}

		c.ConnConfig.DialFunc = func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return client.Dial(network, addr)
		}
	}

	// If sslmode was set in config file or cli argument, set it in the
	// environment so we can use pgx.ParseEnvLibpq to use pgx's built-in
	// functionality.
	switch c.SslMode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		if err := os.Setenv("PGHOST", c.ConnConfig.Host); err != nil {
			return nil, err
		}
		if err := os.Setenv("PGSSLMODE", c.SslMode); err != nil {
			return nil, err
		}
		if err := os.Setenv("PGSSLROOTCERT", c.SslRootCert); err != nil {
			return nil, err
		}

		if cc, err := pgx.ParseConfig(""); err == nil {
			c.ConnConfig.TLSConfig = cc.TLSConfig
			c.ConnConfig.Fallbacks = cc.Fallbacks
		} else {
			return nil, err
		}
	}

	return pgx.ConnectConfig(ctx, &c.ConnConfig)
}

func main() {
	cmdInit := &cobra.Command{
		Use:   "init DIRECTORY",
		Short: "Initialize a new pggo project",
		Long:  "Initialize a new pggo project in DIRECTORY",
		Run:   Init,
	}

	cmdMigrate := &cobra.Command{
		Use:   "migrate [migrationName]",
		Short: "Migrate the database up/down to migration name",
		Long: `Migrate the database to destination migration name.

Destination migration name is optional name of migration reflects schema version:

string - represent name of migration:
  Migrate to a specific migration.
  e.g. pggo migrate 001_create1

nothing:
Migrate to the most recent migration.
  e.g. pggo migrate 
`,
		Run: Migrate,
	}
	// cmdMigrate.Flags().StringVarP(&cliOptions.destinationVersion, "destination", "d", "last", "destination migration version")
	cmdMigrate.Flags().BoolVarP(
		&cliOptions.fakeMigration,
		"fake", "f", false,
		"only mark migration as applied no actual migration(default false)",
	)
	addConfigFlagsToCommand(cmdMigrate)

	cmdStatus := &cobra.Command{
		Use:   "status",
		Short: "Print current migration status",
		Run:   Status,
	}
	addConfigFlagsToCommand(cmdStatus)

	cmdNew := &cobra.Command{
		Use:   "new NAME",
		Short: "Generate a new migration",
		Long:  "Generate a new migration with the next sequence number and provided name",
		Run:   NewMigration,
	}
	cmdNew.Flags().StringVarP(&cliOptions.migrationsPath, "migrations", "m", ".", "migrations path")

	cmdVersion := &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("pggo v%s\n", VERSION)
		},
	}

	rootCmd := &cobra.Command{Use: "pggo", Short: "pggo - PostgreSQL database migrator"}
	rootCmd.AddCommand(cmdInit)
	rootCmd.AddCommand(cmdMigrate)
	rootCmd.AddCommand(cmdStatus)
	rootCmd.AddCommand(cmdNew)
	rootCmd.AddCommand(cmdVersion)
	rootCmd.Execute()
}

func addConfigFlagsToCommand(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&cliOptions.migrationsPath, "migrations", "m", ".", "migrations path")
	cmd.Flags().StringVarP(&cliOptions.configPath, "config", "c", "", "config path (default is ./pggo.conf)")

	cmd.Flags().StringVarP(&cliOptions.host, "host", "", "", "database host")
	cmd.Flags().Uint16VarP(&cliOptions.port, "port", "", 0, "database port")
	cmd.Flags().StringVarP(&cliOptions.user, "user", "", "", "database user")
	cmd.Flags().StringVarP(&cliOptions.password, "password", "", "", "database password")
	cmd.Flags().StringVarP(&cliOptions.database, "database", "", "", "database name")
	cmd.Flags().StringVarP(&cliOptions.sslmode, "sslmode", "", "", "SSL mode")
	cmd.Flags().StringVarP(&cliOptions.sslrootcert, "sslrootcert", "", "", "SSL root certificate")
	cmd.Flags().StringVarP(&cliOptions.versionTable, "version-table", "", "", "version table name (default is public.schema_version)")

	cmd.Flags().StringVarP(&cliOptions.sshHost, "ssh-host", "", "", "SSH tunnel host")
	cmd.Flags().StringVarP(&cliOptions.sshPort, "ssh-port", "", "ssh", "SSH tunnel port")
	cmd.Flags().StringVarP(&cliOptions.sshUser, "ssh-user", "", "", "SSH tunnel user (default is OS user")
	cmd.Flags().StringVarP(&cliOptions.sshPassword, "ssh-password", "", "", "SSH tunnel password (unneeded if using SSH agent authentication)")
}

func Init(cmd *cobra.Command, args []string) {
	var directory string
	switch len(args) {
	case 0:
		directory = "."
	case 1:
		directory = args[0]
		err := os.Mkdir(directory, os.ModePerm)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		cmd.Help()
		os.Exit(1)
	}

	// Write default conf file
	confPath := filepath.Join(directory, "pggo.conf")
	confFile, err := os.OpenFile(confPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer confFile.Close()

	_, err = confFile.WriteString(defaultConf)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Write sample migration
	smPath := filepath.Join(directory, "001_create_people.sql.example")
	smFile, err := os.OpenFile(smPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer smFile.Close()

	_, err = smFile.WriteString(sampleMigration)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func NewMigration(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Help()
		os.Exit(1)
	}

	name := args[0]

	migrationsPath := cliOptions.migrationsPath
	migrations, err := migrate.FindMigrations(migrationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading migrations:\n  %v\n", err)
		os.Exit(1)
	}

	newMigrationName := fmt.Sprintf("%03d_%s.sql", len(migrations)+1, name)

	// Write new migration
	mPath := filepath.Join(migrationsPath, newMigrationName)
	mFile, err := os.OpenFile(mPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer mFile.Close()

	_, err = mFile.WriteString(newMigrationText)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}

func Migrate(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	config, err := LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config:\n  %v\n", err)
		os.Exit(1)
	}

	err = config.Validate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config:\n  %v\n", err)
		os.Exit(1)
	}

	conn, err := config.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to PostgreSQL:\n  %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	migrator, err := migrate.NewMigrator(ctx, conn, config.VersionTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing migrator:\n  %v\n", err)
		os.Exit(1)
	}
	migrator.Data = config.Data

	migrationsPath := cliOptions.migrationsPath
	err = migrator.LoadMigrations(migrationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading migrations:\n  %v\n", err)
		os.Exit(1)
	}
	if len(migrator.Migrations) == 0 {
		fmt.Fprintln(os.Stderr, "No migrations found")
		os.Exit(1)
	}

	migrator.OnStart = func(sequence int32, name, direction, sql string) {
		fmt.Printf("%s executing %s %s\n\n", time.Now().Format("2006-01-02 15:04:05"), name, direction)
	}

	destination := ""
	if len(args) == 1 {
		destination = args[0]
	}
	if cliOptions.fakeMigration {
		migrator.EnableFake()
	}
	if destination == "" {
		err = migrator.Migrate(ctx)
	} else {
		err = migrator.MigrateTo(ctx, destination)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)

		if err, ok := err.(migrate.MigrationPgError); ok {
			if err.Detail != "" {
				fmt.Println("DETAIL:", err.Detail)
			}

			if err.Position != 0 {
				ele, err := ExtractErrorLine(err.Sql, int(err.Position))
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}

				prefix := fmt.Sprintf("LINE %d: ", ele.LineNum)
				fmt.Printf("%s%s\n", prefix, ele.Text)

				padding := strings.Repeat(" ", len(prefix)+ele.ColumnNum-1)
				fmt.Printf("%s^\n", padding)
			}
		}
		os.Exit(1)
	}
}

func Status(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	config, err := LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config:\n  %v\n", err)
		os.Exit(1)
	}

	err = config.Validate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config:\n  %v\n", err)
		os.Exit(1)
	}

	conn, err := config.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to PostgreSQL:\n  %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	migrator, err := migrate.NewMigrator(ctx, conn, config.VersionTable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing migrator:\n  %v\n", err)
		os.Exit(1)
	}
	migrator.Data = config.Data

	migrationsPath := cliOptions.migrationsPath
	err = migrator.LoadMigrations(migrationsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading migrations:\n  %v\n", err)
		os.Exit(1)
	}
	if len(migrator.Migrations) == 0 {
		fmt.Fprintln(os.Stderr, "No migrations found")
		os.Exit(1)
	}

	migrationVersion, err := migrator.GetCurrentVersion(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error retrieving migration version:\n  %v\n", err)
		os.Exit(1)
	}

	var status string
	behindCount := len(migrator.Migrations) - len(migrationVersion)
	if behindCount == 0 {
		status = "up to date"
	} else {
		status = fmt.Sprintf(
			"migration(s) pending - %d",
			len(migrator.Migrations)-len(migrationVersion),
		)
	}

	fmt.Println("status:  ", status)
	if behindCount > 0 {
		fmt.Println("pending migrations:    ")
		migrations, err := migrator.MigrationsToApply(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error retrieving migration version:\n  %v\n", err)
			os.Exit(1)
		}
		for i, m := range migrations {
			fmt.Println("         ", m)
			if i > 2 {
				break
			}
		}
	}
	// fmt.Printf("version:  %d of %d\n", migrationVersion, len(migrator.Migrations))
	fmt.Println("host:    ", config.ConnConfig.Host)
	fmt.Println("database:", config.ConnConfig.Database)
}

func LoadConfig() (*Config, error) {
	config := &Config{VersionTable: "public.schema_version"}
	if connConfig, err := pgx.ParseConfig(""); err == nil {
		config.ConnConfig = *connConfig
	} else {
		return nil, err
	}

	// Set default config path only if it exists
	if cliOptions.configPath == "" {
		if _, err := os.Stat("./pggo.conf"); err == nil {
			cliOptions.configPath = "./pggo.conf"
		}
	}

	if cliOptions.configPath != "" {
		err := appendConfigFromFile(config, cliOptions.configPath)
		if err != nil {
			return nil, err
		}
	}

	appendConfigFromCLIArgs(config)

	if config.SSHConnConfig.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		config.SSHConnConfig.User = user.Username
	}

	if config.SSHConnConfig.Port == "" {
		config.SSHConnConfig.Port = "ssh"
	}

	return config, nil
}

func appendConfigFromFile(config *Config, path string) error {
	env := make(map[string]string)
	for _, s := range os.Environ() {
		parts := strings.SplitN(s, "=", 2)
		env[parts[0]] = parts[1]
	}

	fileBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	confTemplate, err := template.New("conf").Parse(string(fileBytes))
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	err = confTemplate.Execute(&buf, map[string]interface{}{
		"env": env,
	})
	if err != nil {
		return err
	}

	file, err := ini.Load(&buf)
	if err != nil {
		return err
	}

	if host, ok := file.Get("database", "host"); ok {
		config.ConnConfig.Host = host
	}

	// For backwards compatibility if host isn't set look for socket.
	if config.ConnConfig.Host == "" {
		if socket, ok := file.Get("database", "socket"); ok {
			config.ConnConfig.Host = socket
		}
	}

	if p, ok := file.Get("database", "port"); ok {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return err
		}
		config.ConnConfig.Port = uint16(n)
	}
	if database, ok := file.Get("database", "database"); ok {
		config.ConnConfig.Database = database
	}

	if user, ok := file.Get("database", "user"); ok {
		config.ConnConfig.User = user
	}
	if password, ok := file.Get("database", "password"); ok {
		config.ConnConfig.Password = password
	}

	if vt, ok := file.Get("database", "version_table"); ok {
		config.VersionTable = vt
	}

	if sslmode, ok := file.Get("database", "sslmode"); ok {
		config.SslMode = sslmode
	}

	if sslrootcert, ok := file.Get("database", "sslrootcert"); ok {
		config.SslRootCert = sslrootcert
	}

	config.Data = make(map[string]interface{})
	for key, value := range file["data"] {
		config.Data[key] = value
	}

	if host, ok := file.Get("ssh-tunnel", "host"); ok {
		config.SSHConnConfig.Host = host
	}

	if port, ok := file.Get("ssh-tunnel", "port"); ok {
		config.SSHConnConfig.Port = port
	}

	if user, ok := file.Get("ssh-tunnel", "user"); ok {
		config.SSHConnConfig.User = user
	}

	if password, ok := file.Get("ssh-tunnel", "password"); ok {
		config.SSHConnConfig.Password = password
	}

	return nil
}

func appendConfigFromCLIArgs(config *Config) {
	if cliOptions.host != "" {
		config.ConnConfig.Host = cliOptions.host
	}
	if cliOptions.port != 0 {
		config.ConnConfig.Port = cliOptions.port
	}
	if cliOptions.database != "" {
		config.ConnConfig.Database = cliOptions.database
	}
	if cliOptions.user != "" {
		config.ConnConfig.User = cliOptions.user
	}
	if cliOptions.password != "" {
		config.ConnConfig.Password = cliOptions.password
	}
	if cliOptions.sslmode != "" {
		config.SslMode = cliOptions.sslmode
	}
	if cliOptions.sslrootcert != "" {
		config.SslRootCert = cliOptions.sslrootcert
	}
	if cliOptions.versionTable != "" {
		config.VersionTable = cliOptions.versionTable
	}

	if cliOptions.sshHost != "" {
		config.SSHConnConfig.Host = cliOptions.sshHost
	}
	if cliOptions.sshPort != "" {
		config.SSHConnConfig.Port = cliOptions.sshPort
	}
	if cliOptions.sshUser != "" {
		config.SSHConnConfig.User = cliOptions.sshUser
	}
	if cliOptions.sshPassword != "" {
		config.SSHConnConfig.Password = cliOptions.sshPassword
	}
}
