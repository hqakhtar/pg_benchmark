#!/usr/bin/env bash
set -euo pipefail

name="${0##*/}"
case "$name" in
    hammerdbcli)
        [[ "$#" == 2 && "$1" == auto && -f "$2" ]] ||
        {
            printf 'Invalid HammerDB script path\n' >&2
            exit 65
        }

        printf '%s\n' "$$" >"$TEST_ROOT/hammerdb.pid"
        exec "$TEST_TCLSH" "$TEST_PROJECT_ROOT/tests/fixtures/hammerdb-cli.tcl" "$2"
        ;;
    psql)
        database="" user="" sql_file="" query=""
        error_stop=false
        while (($#)); do
            case "$1" in
                --dbname=*) database="${1#*=}" ;;
                --username=*) user="${1#*=}" ;;
                -f) sql_file="$2"; shift ;;
                -Atc) query="$2"; shift ;;
                ON_ERROR_STOP=1) error_stop=true ;;
            esac
            shift
        done
        printf 'psql|database=%s|user=%s|file=%s|query=%s\n' \
            "$database" "$user" "${sql_file##*/}" "$query" >>"$TEST_TRACE"
        [[ "$error_stop" == true ]] || exit 63
        [[ "${TEST_PSQL_FAIL:-false}" == false ]] || exit 39
        if [[ -n "$sql_file" ]];
        then
            [[ -r "$sql_file" ]] || exit 66
            [[ "${TEST_SQL_FILE_FAIL:-}" != "$sql_file" ]] || exit 39
        elif [[ "$query" == 'SHOW server_version' ]];
        then
            printf '17.42-test-server\n'
        else
            cat >/dev/null
        fi

        ;;
    pg_config)
        printf 'pg_config|%s\n' "$*" >>"$TEST_TRACE"
        case "$1" in
            --bindir) printf '%s\n' "$TEST_BIN" ;;
            --libdir) printf '%s\n' "$TEST_BIN" ;;
            --version) printf 'PostgreSQL 99.0-test-client\n' ;;
            *) exit 64 ;;
        esac
        ;;
    initdb)
        data=""
        while (($#)); do
            if [[ "$1" == -D ]];
            then
                data="$2";
                shift;
            fi

            shift
        done
        printf 'initdb|%s\n' "$data" >>"$TEST_TRACE"
        [[ "${TEST_INITDB_FAIL:-false}" == false ]] || exit 38
        mkdir -p -- "$data"
        ;;
    pg_ctl)
        data="" action="" options=""
        while (($#)); do
            case "$1" in
                -D) data="$2"; shift ;;
                -o) options="$2"; shift ;;
                start|stop) action="$1" ;;
            esac
            shift
        done
        printf 'pg_ctl|%s|%s|%s\n' "$action" "$data" "$options" >>"$TEST_TRACE"
        if [[ "$action" == start ]];
        then
            printf 'fake-server\n' >"$data/postmaster.pid"
            [[ "${TEST_PG_START_FAIL:-false}" == false ]] || exit 40
        else
            [[ "${TEST_PG_STOP_FAIL:-false}" == false ]] || exit 41
            rm -- "$data/postmaster.pid"
        fi

        ;;
    tee)
        if [[ "${TEST_LOGGER_FAIL_EARLY:-false}" == true && "${1:-}" == */output.log ]];
        then
            IFS= read -r line
            printf '%s\n' "$line" >"$1"
            printf 'Synthetic log-writer failure\n' >&2
            exit 74
        fi

        "$TEST_REAL_TEE" "$@"
        if [[ "${TEST_LOGGER_FAIL:-false}" == true && "${1:-}" == */output.log ]];
        then
            exit 74
        fi

        ;;
    *) printf 'Unknown fixture executable: %s\n' "$name" >&2; exit 64 ;;
esac
