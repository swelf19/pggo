# Pggo - The SQL Fan's Migrator

fork of https://github.com/jackc/Pggo/
Pggo is a standalone migration tool for PostgreSQL.

## Features

* Multi-platform
* Stand-alone binary
* SSH tunnel support built-in
* Data variable interpolation into migrations

## Installation

    go get -u github.com/jackc/pggo

## Creating a Pggo Project

To create a new pggo project in the current directory run:

    pggo init

Or to create the project somewhere else:

    pggo init path/to/project

Pggo projects are composed of a directory of migrations and optionally a
config file. See the sample directory for an example.

# Configuration

Database connection settings can be specified via the standard PostgreSQL
environment variables, via program arguments, or in a config file. By
default pggo will look in the current directory for the config file pggo.conf
and the migrations.

The `pggo.conf` file is stored in the `ini` format with two sections,
`database` and `data`. The `database` section contains settings for connection
to the database server.

Values in the `data` section will be available for interpolation into
migrations. This can help in scenarios where migrations are managing
permissions and the user to which permissions are granted should be
configurable.

If all database settings are supplied by PG* environment variables or program
arguments the config file is not required. In particular, using the `PGSERVICE`
can reduce or eliminate the need for a configuration file.

The entire `pggo.conf` file is processed through the Go standard
`text/template` package. The program environment is available at `.env`.

Example `pggo.conf`:

```ini
[database]
# host supports TCP addresses and Unix domain sockets
# host = /private/tmp
host = 127.0.0.1
# port = 5432
database = pggo_test
user = jack
password = {{.env.MIGRATOR_PASSWORD}}
# version_table = public.schema_version
#
# sslmode generally matches the behavior described in:
# http://www.postgresql.org/docs/9.4/static/libpq-ssl.html#LIBPQ-SSL-PROTECTION
#
# There are only two modes that most users should use:
# prefer - on trusted networks where security is not required
# verify-full - require SSL connection
# sslmode = prefer

# Proxy the above database connection via SSH
# [ssh-tunnel]
# host =
# port = 22
# user defaults to OS user
# user =
# password is not required if using SSH agent authentication
# password =

[data]
prefix = foo
app_user = joe

```

This flexibility configuration style allows handling multiple environments such
as test, development, and production in several ways.

* Separate config file for each environment
* Environment variables for database settings and optionally one config file
  for shared settings
* Program arguments for database settings and optionally one config file for
  shared settings

## Migrations

To create a new migration:

    pggo new name_of_migration

This will create a migration file with the given name prefixed by the next available sequence number (e.g. 001, 002, 003).

The migrations themselves have an extremely simple file format. They are
simply the up and down SQL statements divided by a magic comment.

    ---- create above / drop below ----

Example:

```sql
create table t1(
  id serial primary key
);

---- create above / drop below ----

drop table t1;
```

If a migration is irreversible such as a drop table, simply delete the magic
comment.

```sql
drop table widgets;
```

To interpolate a custom data value from the config file prefix the name with a
dot and surround the whole with double curly braces.

```sql
create table {{.prefix}}config(
  id serial primary key
);
```

Migrations are read from files in the migration directory in the order of the
numerical prefix. Each migration is run in a transaction.

Any SQL files in subdirectories of the migration directory, will be available
for inclusion with the template command. This can be especially useful for
definitions of views and functions that may have to be dropped and recreated
when the underlying table(s) change.

```sql
// Include the file shared/v1_001.sql. Note the trailing dot.
// It is necessary if the shared file needs access to custom data values.
{{ template "shared/v1_001.sql" . }}
);
```

Pggo uses the standard Go
[text/template](http://golang.org/pkg/text/template/) package so conditionals
and other advanced templating features are available if needed. See the
package docs for details.

## Migrating

To migrate up to the last version using migrations and config file located in
the same directory simply run pggo:

    pggo migrate

To migrate up or down to a specific version:

    pggo migrate --destination 42

To migrate up N versions:

    pggo migrate --destination +3

To migrate down N versions:

    pggo migrate --destination -3

To migrate down and rerun the previous N versions:

    pggo migrate --destination -+3

To use a different config file:

    pggo migrate --config path/to/pggo.json

To use a different migrations directory:

    pggo migrate --migrations path/to/migrations

## SSH Tunnel

Pggo includes SSH tunnel support. Simply supply the SSH host, and optionally
port, user, and password in the config file or as program arguments and Pggo
will tunnel the database connection through that server. When using a SSH tunnel
the database host should be from the context of the SSH server. For example, if
your PostgreSQL server is `pg.example.com`, but you only have SSH access, then
your SSH host would be pg.example.com and your database host would be
`localhost`.

Pggo will automatically use an SSH agent if available.

## Embedding Pggo

All the actual functionality of pggo is in the github.com/jackc/pggo/migrate
library. If you need to embed migrations into your own application this
library can help.

## Running the Tests

To run the tests pggo requires two test databases to run migrations against.

1. Create a new database for main pggo program tests.
2. Open testdata/pggo.conf.example
3. Enter the connection information.
4. Save as testdata/pggo.conf.
5. Create another database for the migrate library tests.
6. Run tests with the connection string for the migrate library tests in the MIGRATE_TEST_CONN_STRING environment variable

    MIGRATE_TEST_CONN_STRING="host=/private/tmp database=pggo_migrate_test" go test ./...

## Prior Ruby Gem Version

The projects using the prior version of pggo that was distributed as a Ruby
Gem are incompatible with the version 1 release. However, that version of pggo
is still available through RubyGems and the source code is on the ruby branch.

## Version History

## 1.10.1 (March 24, 2020)

* Fix default CLI version-table argument overriding config value

## 1.10.0 (March 7, 2020)

* Better locking to protect against multiple concurrent migrators on first run
* Update pgx version to support PostgreSQL service files

## 1.9.1 (February 1, 2020)

* Look for version table in all schemas in search_path instead of just the top schema

## 1.9.0 (February 1, 2020)

* Default version table is explicitly in public schema
* Update to pgx v4 (Alex Gaynor)

## 1.8.2 (July 19, 2019)

* Show PostgreSQL error details
* Rename inpggoal error type

## 1.8.1 (April 5, 2019)

* Issue `reset all` after every migration
* Use go modules instead of Godep / vendoring

## 1.8.0 (February 26, 2018)

* Update to latest version of pgx (PostgreSQL driver)
* Support PGSSLROOTCERT
* Fix typos and inpggoal cleanup
* Refactor inpggoals for easier embedding (hsyed)

## 1.7.1 (January 30, 2016)

* Simplify SSH tunnel code so it does not listen on localhost

## 1.7.0 (January 17, 2016)

* Add SSH tunnel support

## 1.6.1 (January 16, 2016)

* Fix version output
* Evaluate config files through text/template with ENV

## 1.6.0 (January 15, 2016)

* Optionally read database connection settings from environment
* Accept database connection settings via program arguments
* Make config file optional

## 1.5.0 (October 1, 2015)

* Add status command
* Add relative migration destinations
* Add redo migration destinations

## 1.4.0 (May 15, 2015)

* Add TLS support

## 1.3.3 (May 1, 2015)

* Fix version output

## 1.3.2 (May 1, 2015)

* Better error messages

## 1.3.1 (December 24, 2014)

* Fix custom version table name

## 1.3.0 (December 23, 2014)

* Prefer host config whether connecting with unix domain socket or TCP

## 1.2.2 (May 30, 2014)

* Fix new migration short name

## 1.2.1 (May 18, 2014)

* Support socket directory as well as socket file

## 1.2.0 (May 6, 2014)

* Move to subcommand interface
* Require migrations to begin with ascending, gapless numbers
* Fix: migrations directory can contain other files
* Fix: gracefully handle invalid current version
* Fix: gracefully handle migrations with duplicate sequence number

## 1.1.1 (April 22, 2014)

* Do not require user -- default to OS user

## 1.1.0 (April 22, 2014)

* Add sub-template support
* Switch to ini for config files
* Add custom data merging

## 1.0.0 (April 19, 2014)

* Total rewrite in Go

## 0.7.1

* Print friendly error message when database error occurs instead of stack trace.

## 0.7.0

* Added ERB processing to SQL files

## License

Copyright (c) 2011-2014 Jack Christensen, released under the MIT license
