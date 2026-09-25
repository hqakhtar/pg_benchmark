#!/usr/bin/env bash

postgresql_validate()
{
    [[ -n "$PGHOST" && -n "$PGDATABASE" && -n "$PGUSER" && -n "$PGMAINTENANCE_DB" ]] ||
    {
        fail "PGHOST, PGDATABASE, PGUSER, and PGMAINTENANCE_DB must not be empty"
        return 1
    }

    local database
    for database in "$PGDATABASE" "$PGMAINTENANCE_DB"; do
        case "$database" in
            *'='*|postgres://*|postgresql://*)
                fail "Database settings must be names, not connection strings"
                return 1
                ;;
        esac
    done
    case "$PGDATABASE" in
        template0|template1) fail "A PostgreSQL template database cannot be a benchmark target"; return 1 ;;
    esac
    validate_integer PGPORT "$PGPORT" 1 65535
    validate_integer PGCONNECT_TIMEOUT "$PGCONNECT_TIMEOUT" 1
    validate_boolean PG_REMOVE_DATA "$PG_REMOVE_DATA"
    case "$PGSSLMODE" in
        disable|allow|prefer|require|verify-ca|verify-full) ;;
        *) fail "Invalid PGSSLMODE: $PGSSLMODE"; return 1 ;;
    esac

    # Existing targets need only a client; server tools are for owned clusters.
    case "$PG_TARGET_MODE" in
        existing)
            require_command "$PG_PSQL"
            PG_PSQL="$(command -v -- "$PG_PSQL")"
            ;;
        temporary)
            case "$PGHOST" in
                localhost|127.0.0.1) ;;
                *) fail "A temporary cluster requires PGHOST=localhost or 127.0.0.1"; return 1 ;;
            esac
            if [[ "$RUN_ACTION" == cleanup ||
                  ( "$RUN_ACTION" == run && "$RUN_PREPARE_MODE" == reuse ) ]];
            then
                fail "A new temporary cluster must be prepared; use RUN_PREPARE_MODE=once or each"
                return 1
            fi

            [[ "$RUN_ACTION" != prepare || "$PG_REMOVE_DATA" == false ]] ||
            {
                fail "Prepare-only with PG_REMOVE_DATA=true would discard the prepared database"
                return 1
            }

            PG_CONFIG="${PG_CONFIG:-pg_config}"
            require_command "$PG_CONFIG"
            PG_CONFIG="$(command -v -- "$PG_CONFIG")"
            PG_BIN_DIR="$("$PG_CONFIG" --bindir)"
            PG_BIN_DIR="$(absolute_path "$PG_BIN_DIR")"
            local library_directory
            library_directory="$("$PG_CONFIG" --libdir)"
            library_directory="$(absolute_path "$library_directory")"
            export LD_LIBRARY_PATH="$library_directory${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            local executable
            for executable in initdb pg_ctl psql; do
                [[ -x "$PG_BIN_DIR/$executable" ]] ||
                {
                    fail "Missing PostgreSQL executable: $PG_BIN_DIR/$executable"
                    return 1
                }

            done
            PG_PSQL="$PG_BIN_DIR/psql"
            if [[ -n "$PG_SERVER_ENV_FILE" ]];
            then
                PG_SERVER_ENV_FILE="$(environment_path "$PG_SERVER_ENV_FILE")" || return 1
            fi

            if [[ -n "$PG_INIT_SQL" ]];
            then
                PG_INIT_SQL="$(absolute_path "$PG_INIT_SQL")"
                [[ -r "$PG_INIT_SQL" ]] ||
                {
                    fail "Initialization SQL not readable: $PG_INIT_SQL"
                    return 1
                }

            fi

            ;;
        *) fail "PG_TARGET_MODE must be existing or temporary"; return 1 ;;
    esac
    # Debian's psql symlink dispatches by argv[0]; preserve its basename.
    PG_PSQL="$(absolute_path "$(dirname -- "$PG_PSQL")")/$(basename -- "$PG_PSQL")"
    [[ -x "$PG_PSQL" ]] || { fail "PG_PSQL is not an executable file: $PG_PSQL"; return 1; }

    export PG_PSQL PG_CONFIG PG_SERVER_ENV_FILE PG_INIT_SQL
}

postgresql_exec()
{
    local database="$1" user="$2"
    shift 2
    # Pin the target and disable password prompts and user psql startup files.
    "$PG_PSQL" -X -w -v ON_ERROR_STOP=1 \
        --host="$PGHOST" --port="$PGPORT" --username="$user" \
        --dbname="$database" "$@"
}

postgresql_start()
{
    PG_SERVER_OWNED=false
    [[ "$PG_TARGET_MODE" == temporary ]] || return 0
    PG_DATA_DIR="$RUN_DIRECTORY/postgresql/data"
    mkdir -p -- "$RUN_DIRECTORY/postgresql"
    "$PG_BIN_DIR/initdb" -D "$PG_DATA_DIR" -U "$PGUSER" --auth=trust \
        >"$RUN_DIRECTORY/postgresql/initdb.log" 2>&1
    if [[ -n "$PG_SERVER_ENV_FILE" ]];
    then
        load_environment "$PG_SERVER_ENV_FILE"
    fi

    # Ownership is established before start, so a partial start is also stopped.
    PG_SERVER_OWNED=true
    "$PG_BIN_DIR/pg_ctl" -D "$PG_DATA_DIR" \
        -l "$RUN_DIRECTORY/postgresql/server.log" \
        -o "${PG_SERVER_OPTIONS:-} -c listen_addresses=127.0.0.1 -c unix_socket_directories='' -p $PGPORT" \
        -w -t 60 start
    postgresql_exec "$PGMAINTENANCE_DB" "$PGUSER" --set=benchmark_database="$PGDATABASE" \
        >"$RUN_DIRECTORY/postgresql/database.log" 2>&1 <<'SQL'
SELECT format('CREATE DATABASE %I', :'benchmark_database')
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = :'benchmark_database'
)
\gexec
SQL
    if [[ -n "$PG_INIT_SQL" ]];
    then
        postgresql_exec "$PGMAINTENANCE_DB" "$PGUSER" -f "$PG_INIT_SQL" \
            >"$RUN_DIRECTORY/postgresql/init-sql.log" 2>&1
    fi
}

postgresql_stop()
{
    local status="$1"
    [[ "${PG_SERVER_OWNED:-false}" == true ]] || return 0
    if [[ -f "$PG_DATA_DIR/postmaster.pid" ]];
    then
        "$PG_BIN_DIR/pg_ctl" -D "$PG_DATA_DIR" -m fast -w -t 60 stop ||
        {
            fail "Could not stop the owned PostgreSQL cluster; preserving $PG_DATA_DIR"
            return 1
        }

    fi

    PG_SERVER_OWNED=false
    # Retain failed runs for diagnosis, even when successful runs delete data.
    if [[ "$status" == 0 && "$PG_REMOVE_DATA" == true ]];
    then
        [[ "$PG_DATA_DIR" == "$RUN_DIRECTORY/postgresql/data" ]] ||
        {
            fail "Refusing to remove an unowned data directory"
            return 1
        }

        rm -r -- "$PG_DATA_DIR"
    fi
}

postgresql_record_metadata()
{
    local version
    # Record the target's version, which can differ from the local client.
    version="$(postgresql_exec "$PGDATABASE" "$PGUSER" -Atc 'SHOW server_version')"
    [[ -n "$version" ]] || { fail "The target returned no PostgreSQL version"; return 1; }

    {
        printf '{\n'
        json_field host "$PGHOST"
        json_field port "$PGPORT"
        json_field database "$PGDATABASE"
        json_field user "$PGUSER"
        json_field sslmode "$PGSSLMODE"
        json_field maintenance_database "$PGMAINTENANCE_DB"
        printf '  "connect_timeout_seconds": %s,\n' "$PGCONNECT_TIMEOUT"
        json_field server_version "$version" ''
        printf '}\n'
    } >"$RUN_DIRECTORY/target.json"
}
