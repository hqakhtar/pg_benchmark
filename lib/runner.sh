#!/usr/bin/env bash

runner_validate()
{
    validate_integer RUN_ITERATIONS "${RUN_ITERATIONS:?Load run.env before invoking the runner}" 1
    validate_integer RUN_COOLDOWN_SECONDS "$RUN_COOLDOWN_SECONDS" 0
    validate_integer RUN_TIMEOUT_SECONDS "$RUN_TIMEOUT_SECONDS" 0
    validate_boolean RUN_ALLOW_DESTRUCTIVE "$RUN_ALLOW_DESTRUCTIVE"
    case "$RUN_PREPARE_MODE" in
        reuse|once|each) ;;
        *) fail "RUN_PREPARE_MODE must be reuse, once, or each"; return 1 ;;
    esac
    [[ "$RUN_LABEL" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] ||
    {
        fail "RUN_LABEL must be 1-64 letters, digits, dots, underscores, or hyphens"
        return 1
    }

    [[ -n "$RUN_OUTPUT_ROOT" ]] || { fail "RUN_OUTPUT_ROOT must not be empty"; return 1; }

    RUN_OUTPUT_ROOT="$(absolute_path "$RUN_OUTPUT_ROOT")"
    export RUN_OUTPUT_ROOT
    if [[ "$RUN_ACTION" == cleanup && "$RUN_ALLOW_DESTRUCTIVE" != true ]];
    then
        fail "Cleanup requires RUN_ALLOW_DESTRUCTIVE=true and a dedicated benchmark database"
        return 1
    fi

    local command hook
    for command in setsid timeout tee mkfifo; do
        require_command "$command"
    done
    # Check the adapter contract before creating any run output.
    for hook in benchmark_validate benchmark_prepare benchmark_run benchmark_cleanup benchmark_describe; do
        declare -F "$hook" >/dev/null ||
        {
            fail "Benchmark module does not implement $hook"
            return 1
        }

    done
}

runner_manifest()
{
    local status="$1" exit_code="$2"
    {
        printf '{\n'
        json_field id "$RUN_ID"
        json_field benchmark "$BENCHMARK_TYPE"
        json_field action "$RUN_ACTION"
        json_field status "$status"
        json_field started_at "$RUN_STARTED_AT"
        json_field updated_at "$(utc_now)"
        json_field connection_config "$CONNECTION_CONFIG"
        json_field benchmark_config "$BENCHMARK_CONFIG"
        json_field run_config "$RUN_CONFIG"
        json_field prepare_mode "$RUN_PREPARE_MODE"
        printf '  "cooldown_seconds": %s,\n' "$RUN_COOLDOWN_SECONDS"
        printf '  "timeout_seconds": %s,\n' "$RUN_TIMEOUT_SECONDS"
        printf '  "allow_destructive": %s,\n' "$RUN_ALLOW_DESTRUCTIVE"
        json_field target_mode "$PG_TARGET_MODE"
        printf '  "iterations": %s,\n' "$RUN_ITERATIONS"
        printf '  "completed_iterations": %s,\n' "$RUN_COMPLETED_ITERATIONS"
        printf '  "exit_code": %s\n}\n' "$exit_code"
    } >"$RUN_DIRECTORY/run.json.tmp" || return 1

    # Replace atomically so readers never see a partially written manifest.
    mv -- "$RUN_DIRECTORY/run.json.tmp" "$RUN_DIRECTORY/run.json"
}

runner_stop_child()
{
    RUN_LOG_RESULT=0
    if [[ -n "${RUN_ACTIVE_PID:-}" ]];
    then
        # The adapter and all of its children have their own process group.
        if kill -0 -- "-$RUN_ACTIVE_PID" 2>/dev/null;
        then
            kill -TERM -- "-$RUN_ACTIVE_PID" 2>/dev/null || :
            local attempt
            for ((attempt = 0; attempt < 20; attempt++)); do
                kill -0 -- "-$RUN_ACTIVE_PID" 2>/dev/null || break
                sleep 0.1
            done
            if kill -0 -- "-$RUN_ACTIVE_PID" 2>/dev/null;
            then
                kill -KILL -- "-$RUN_ACTIVE_PID" 2>/dev/null || :
            fi

        fi

        wait "$RUN_ACTIVE_PID" 2>/dev/null || :
        RUN_ACTIVE_PID=""
    fi

    if [[ -n "${RUN_LOG_PID:-}" ]];
    then
        wait "$RUN_LOG_PID" 2>/dev/null || RUN_LOG_RESULT=$?
        RUN_LOG_PID=""
    fi

    if [[ -n "${RUN_OUTPUT_FIFO:-}" && -p "$RUN_OUTPUT_FIFO" ]];
    then
        rm -- "$RUN_OUTPUT_FIFO"
    fi
}

runner_finish()
{
    local status="$1" final_status
    trap - EXIT INT TERM HUP
    runner_stop_child
    # Failed shutdown must not leave an otherwise successful run marked successful.
    if ! postgresql_stop "$status";
    then
        status=1
    fi

    final_status=failed
    if [[ "$status" == 0 ]];
    then
        final_status=succeeded
    elif [[ "$status" == 129 || "$status" == 130 || "$status" == 143 ]];
    then
        final_status=interrupted
    fi

    if ! runner_manifest "$final_status" "$status";
    then
        printf 'ERROR: Could not write run status in %s\n' "$RUN_DIRECTORY" >&2
        status=1
    fi

    printf 'Run %s: %s (exit %s)\n' "$final_status" "$RUN_DIRECTORY" "$status"
    exit "$status"
}

runner_stage()
{
    local hook="$1" directory="$2" result=0 first_status=0
    local started_at start_seconds="$SECONDS"
    started_at="$(utc_now)"
    mkdir -p -- "$directory"
    # The FIFO lets the runner supervise the adapter and logger independently.
    RUN_OUTPUT_FIFO="$directory/.output"
    mkfifo -- "$RUN_OUTPUT_FIFO"
    printf 'Starting %s; log: %s/output.log\n' "$hook" "$directory"
    tee "$directory/output.log" <"$RUN_OUTPUT_FIFO" &
    RUN_LOG_PID=$!
    local -a command=("$BASH" "$FRAMEWORK_ROOT/lib/adapter-exec.sh" "$BENCHMARK_MODULE" "$hook" "$directory")
    if ((RUN_TIMEOUT_SECONDS > 0));
    then
        command=(timeout --kill-after=5 "$RUN_TIMEOUT_SECONDS" "${command[@]}")
    fi

    setsid "${command[@]}" >"$RUN_OUTPUT_FIFO" 2>&1 &
    RUN_ACTIVE_PID=$!
    # A failed logger must not leave a silent, still-running workload behind.
    wait -n || first_status=$?
    if ((first_status == 0 || first_status == 127));
    then
        wait "$RUN_ACTIVE_PID" || result=$?
    else
        result="$first_status"
    fi

    runner_stop_child
    if ((result == 0 && RUN_LOG_RESULT != 0));
    then
        result="$RUN_LOG_RESULT"
    fi

    if ((RUN_LOG_RESULT != 0));
    then
        printf 'ERROR: Log writer failed (exit %s) for %s\n' "$RUN_LOG_RESULT" "$directory" >&2
    fi

    {
        printf '{\n'
        json_field operation "$hook"
        json_field started_at "$started_at"
        json_field finished_at "$(utc_now)"
        printf '  "elapsed_seconds": %s,\n' "$((SECONDS - start_seconds))"
        printf '  "exit_code": %s\n}\n' "$result"
    } >"$directory/status.json"

    if ((result != 0));
    then
        printf 'ERROR: %s failed (exit %s); see %s/output.log\n' "$hook" "$result" "$directory" >&2
        return "$result"
    fi
}

runner_main()
{
    # Native-tool logs may contain sensitive diagnostics; keep each run private.
    umask 077
    mkdir -p -- "$RUN_OUTPUT_ROOT"
    RUN_DIRECTORY="$RUN_OUTPUT_ROOT/${RUN_LABEL}-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
    mkdir -- "$RUN_DIRECTORY"
    RUN_ID="${RUN_DIRECTORY##*/}"
    RUN_STARTED_AT="$(utc_now)"
    RUN_COMPLETED_ITERATIONS=0
    RUN_ACTIVE_PID=""
    RUN_LOG_PID=""
    RUN_OUTPUT_FIFO=""
    export RUN_DIRECTORY RUN_ID
    trap 'runner_finish "$?"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap 'exit 129' HUP
    runner_manifest running 0
    benchmark_describe >"$RUN_DIRECTORY/benchmark.json"
    # An owned local cluster spans the whole run, not just one iteration.
    postgresql_start

    case "$RUN_ACTION" in
        prepare)
            runner_stage benchmark_prepare "$RUN_DIRECTORY/prepare"
            postgresql_record_metadata
            ;;
        cleanup)
            postgresql_record_metadata
            runner_stage benchmark_cleanup "$RUN_DIRECTORY/cleanup"
            ;;
        run)
            if [[ "$RUN_PREPARE_MODE" == once ]];
            then
                runner_stage benchmark_prepare "$RUN_DIRECTORY/prepare"
            fi

            local iteration directory
            for ((iteration = 1; iteration <= RUN_ITERATIONS; iteration++)); do
                directory="$RUN_DIRECTORY/iteration-$iteration"
                if [[ "$RUN_PREPARE_MODE" == each ]];
                then
                    runner_stage benchmark_prepare "$directory/prepare"
                fi

                if ((iteration == 1));
                then
                    postgresql_record_metadata
                elif ((RUN_COOLDOWN_SECONDS > 0));
                then
                    sleep "$RUN_COOLDOWN_SECONDS"
                fi

                export RUN_ITERATION="$iteration"
                runner_stage benchmark_run "$directory"
                [[ -s "$directory/result.json" ]] ||
                {
                    fail "Benchmark returned no result.json for iteration $iteration"
                    return 1
                }

                RUN_COMPLETED_ITERATIONS="$iteration"
                runner_manifest running 0
            done
            ;;
    esac
}
