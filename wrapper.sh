#!/bin/bash

# SCRIPT: wrapper.sh
#-----------------------------
# See usage on how to run this script.
#
# * REQUIRES:
#   - $PG_CONF_FILE
#   - $BENCHMARK_SCRIPT

set -o pipefail

## GLOBAL VARIABLES

# Paths and file names
export SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export WORK_DIR=""
export HAMMERDB_INSTALL_DIR=""

# Only supporting hammerdb currently
export BENCHMARK_TYPE="hammerdb"

# Script variables
export ITERATIONS="${ITERATIONS:-3}"
export BENCHMARK_NAME="${BENCHMARK_NAME:-tpcc}"
export PG_CONF_FILE="${PG_CONF_FILE:-$SCRIPT_DIR/pg.env}"
export BENCHMARK_SCRIPT="$BENCHMARK_TYPE/${BENCHMARK_SCRIPT:-$BENCHMARK_TYPE.sh}"
export PG_VERSION=""

export PRELOAD_LIBRARY=""
export PG_INIT_SQL=""
export INITDB=""
export BUILD_SCHEMA=""
export BUILD_SCHEMA_ONCE=""
export PREPARE_ONLY=""
export REMOVE_DATA_DIR=""
export CITUS_COMPAT_MODE=""


## USAGE
usage()
{
    errorCode=${1:-0}

    cat << EOF

usage: $0 OPTIONS

This script runs benchmarking for all of following permutations:
- PG
- Any preload shared library passed as an argument.

By default, this script runs against an EXISTING PostgreSQL cluster. To set up
a new cluster, use the -I (initdb), -S (build schema), and -Z (remove data dir)
options.

It requires an environment file for sourcing PG and TPCC configuration
variables. The default file $PG_CONF_FILE available in $SCRIPT_DIR. It also
expects that $BENCHMARK_SCRIPT resides in the "$SCRIPT_DIR/$BENCHMARK_TYPE"
directory.

Some fo the options can also be set via environment variables. The relevant
variable is provided with each option. However, some arguments are mandatory
to prevent any accidental environment/data corruption.

OPTIONS can be:

  -h  Show this message

  -C  [PG_CONFIG]       pg_config path                   [REQUIRED]
  -H                    HammerDB installation dir        [REQUIRED]
  -t                    Script working folder            [REQUIRED]
                        * a folder where data directory and relevant log
                          files may be created.

  -b  [BENCHMARK_Type]  Type of benchmark to run         [Default: $BENCHMARK_TYPE]
  -c                    Enable Citus compatibility       [Default: not set]
  -e  [PG_CONF_FILE]    PG configuration file.           [Default: $PG_CONF_FILE]
  -I                    Run initdb                       [Default: not set]
  -i  [ITERATIONS]      Number of iterations             [Default: $ITERATIONS]
  -l  [PRELOAD_LIBRARY] Shared preload library           [Default: none]
  -n  [BENCHMARK_NAME]  Benchmark name                   [Default: $BENCHMARK_NAME]
  -O                    Build schema once only           [Default: not set]
                        Runs maintenance script.
  -P                    Prepare only: build schema and   [Default: not set]
                        exit without running benchmarks.
  -r  [PG_INIT_SQL]     SQL script to run after initdb   [Default: none]
  -S                    New schema every iternation      [Default: not set]
  -Z                    Remove data directory            [Default: not set]

EOF

    if [[ $errorCode -ne 0 ]];
    then
        exit_script $errorCode
    fi
}

# Perform any required cleanup and exit with the given error/success code
exit_script()
{
    # Exit with a given return code or 0 if none are provided.
    exit ${1:-0}
}

# Sanity check environment before running benchmarks
sanity_check()
{
    local errors=0

    # PG_NUM_VU (schema build VUs) must be less than warehouse count
    if [[ "$PG_NUM_VU" -ge "$PG_COUNT_WARE" ]]; then
        echo "SANITY CHECK FAILED: PG_NUM_VU ($PG_NUM_VU) must be less than PG_COUNT_WARE ($PG_COUNT_WARE)" >&2
        errors=$((errors + 1))
    fi

    # Check ulimit -n (open files) is more than 5x PG_NUM_VU and 5x PG_VU
    local open_files
    open_files=$(ulimit -n)
    if [[ "$open_files" != "unlimited" ]]; then
        local min_for_num_vu=$((PG_NUM_VU * 5))
        local min_for_vu=$((PG_VU * 5))

        if [[ "$open_files" -le "$min_for_num_vu" ]]; then
            echo "SANITY CHECK FAILED: ulimit -n ($open_files) must be more than 5x PG_NUM_VU ($PG_NUM_VU), i.e. > $min_for_num_vu" >&2
            errors=$((errors + 1))
        fi

        if [[ "$open_files" -le "$min_for_vu" ]]; then
            echo "SANITY CHECK FAILED: ulimit -n ($open_files) must be more than 5x PG_VU ($PG_VU), i.e. > $min_for_vu" >&2
            errors=$((errors + 1))
        fi
    fi

    # Check ulimit -u (max user processes) is large enough
    local max_procs
    max_procs=$(ulimit -u)
    local min_procs=4096
    if [[ "$max_procs" != "unlimited" && "$max_procs" -lt "$min_procs" ]]; then
        echo "SANITY CHECK FAILED: ulimit -u ($max_procs) is too low; should be at least $min_procs or unlimited" >&2
        errors=$((errors + 1))
    fi

    if [[ $errors -gt 0 ]]; then
        echo "Sanity check failed with $errors error(s). Aborting." >&2
        exit_script 1
    fi

    echo "Sanity check passed."
}

# Vaildate arguments to ensure that we can safely run the benchmark
validate_args()
{
    if [[ ! -f "$PG_CONF_FILE" ]];
	then
        echo "Configuration file does not exist. See usage for details" >&2
        usage 1
    fi

    if [[ ! -f "$PG_CONFIG" ]];
    then
        echo "pg_config pathname is required. See usage for details" >&2
        usage 1
    fi

    if [[ ! -d "$HAMMERDB_INSTALL_DIR" ]];
    then
        echo "Incorrect path for hammerdb installation. See usage for details" >&2
        usage 1
    fi

    if [[ ! -d "$WORK_DIR" ]];
    then
        echo "Script working directory for the script is required. See usage for details" >&2
        usage 1
    fi
}

# Clean up existing benchmark data (only when not running initdb)
postgresql_cleanup()
{
	local iteration="$1"
	local btype="$2"

	echo "INITDB not set. Running cleanup script before iteration $iteration..."
	cleanup_log="$WORK_DIR/$BENCHMARK_NAME.cleanup.$btype.$iteration.log"

	cleanup_sql="$SCRIPT_DIR/$BENCHMARK_TYPE/${BENCHMARK_TYPE}_cleanup"
	if [[ ! -z "$CITUS_COMPAT_MODE" ]]; then
		cleanup_sql="${cleanup_sql}_citus"
	fi
	cleanup_sql="${cleanup_sql}.sql"

	psql -v ON_ERROR_STOP=1 -f "$cleanup_sql" 2>&1 | tee "$cleanup_log"
	if [[ $? -ne 0 ]]; then
		echo "Cleanup script failed. See log file [$cleanup_log] for details." >&2
		exit_script 1
	fi
}

# Run maintenance script between iterations (used with -O)
postgresql_maintenance()
{
	local iteration="$1"
	local btype="$2"
    local sleep_duration=60

	echo "Running maintenance script before iteration $iteration..."
    echo "Sleeping for $sleep_duration seconds to allow the system to stabilize before running maintenance tasks..."
    sleep $sleep_duration

	maintenance_log="$WORK_DIR/$BENCHMARK_NAME.maintenance.$btype.$iteration.log"

	maintenance_sql="$SCRIPT_DIR/$BENCHMARK_TYPE/${BENCHMARK_TYPE}_maintenance"
	if [[ ! -z "$CITUS_COMPAT_MODE" ]]; then
		maintenance_sql="${maintenance_sql}_citus"
	fi
	maintenance_sql="${maintenance_sql}.sql"

	psql -v ON_ERROR_STOP=1 -f "$maintenance_sql" 2>&1 | tee "$maintenance_log"
	if [[ $? -ne 0 ]]; then
		echo "Maintenance script failed. See log file [$maintenance_log] for details." >&2
		exit_script 1
	fi

    echo "Sleeping for $sleep_duration seconds to allow the system to stabilize after running maintenance tasks..."
    sleep $sleep_duration
}

# Prepare only: build schema and exit without running benchmarks
prepare_only()
{
	local benchmark_type="$1"
	local data_pg_logs_dir="$WORK_DIR/$benchmark_type/prepare"
	local script_logfile="$WORK_DIR/$BENCHMARK_NAME.$benchmark_type.prepare.log"

	mkdir -p $data_pg_logs_dir

	echo
	echo "================================================================================"
	echo "[$BENCHMARK_NAME: $benchmark_type] Prepare only - building schema"
	echo "================================================================================"
	echo

	INITDB_FLAG=""
	if [[ ! -z "$INITDB" ]]; then
		INITDB_FLAG="-i"
	fi

	REMOVE_DIR_FLAG=""
	if [[ ! -z "$REMOVE_DATA_DIR" ]]; then
		REMOVE_DIR_FLAG="-z"
	fi

	CITUS_FLAG=""
	if [[ ! -z "$CITUS_COMPAT_MODE" ]]; then
		CITUS_FLAG="-c"
	fi

	if [[ ! -z "$PG_INIT_SQL" ]]; then
		$SCRIPT_DIR/$BENCHMARK_SCRIPT $INITDB_FLAG -S -P $REMOVE_DIR_FLAG $CITUS_FLAG -C $PG_CONFIG -t $data_pg_logs_dir -x $HAMMERDB_INSTALL_DIR -r "$PG_INIT_SQL" 2>&1 | tee $script_logfile
	else
		$SCRIPT_DIR/$BENCHMARK_SCRIPT $INITDB_FLAG -S -P $REMOVE_DIR_FLAG $CITUS_FLAG -C $PG_CONFIG -t $data_pg_logs_dir -x $HAMMERDB_INSTALL_DIR 2>&1 | tee $script_logfile
	fi

	if [[ $? -ne 0 ]]; then
		echo "Prepare failed. See log file [$script_logfile] for details." >&2
		exit_script 1
	fi

	echo
	echo "Schema prepared successfully. Exiting without running benchmarks."
	exit_script 0
}

# Benchmarking loop
run_loop()
{
	retval=0
	benchmark_type="$1"
	script_logfile=""
	data_pg_logs_dir=""
	summary_file="$WORK_DIR/$BENCHMARK_NAME.summary.$benchmark_type.log"

	# Empty file
	: > $summary_file 2>/dev/null

	# Run the loop
	for (( i=1; i <= $ITERATIONS; i++ ))
	do
		echo
		echo "================================================================================"
		echo "[$BENCHMARK_NAME: $benchmark_type] Iteration $i of $ITERATIONS"
		echo "================================================================================"
		echo

		# If we are not running initdb, run cleanup or maintenance before each iteration
		if [[ -z "$INITDB" ]]; then
			if [[ ! -z "$BUILD_SCHEMA_ONCE" ]]; then
				# Build once: cleanup before first iteration, maintenance for the rest
				if [[ $i -eq 1 ]]; then
					postgresql_cleanup "$i" "$benchmark_type"
				else
					postgresql_maintenance "$i" "$benchmark_type"
				fi
			elif [[ -z "$BUILD_SCHEMA" ]]; then
				# No schema build: run maintenance
				postgresql_maintenance "$i" "$benchmark_type"
			else
				# Schema rebuilt every iteration: run cleanup
				postgresql_cleanup "$i" "$benchmark_type"
			fi
		fi

		# Set data and log directory for current iteration
		data_pg_logs_dir="$WORK_DIR/$benchmark_type/$i"
		mkdir -p $data_pg_logs_dir

		# Script file for capturing benchmarking script output
		script_logfile=$WORK_DIR/$BENCHMARK_NAME.$benchmark_type.$i.log

		# Run benchmark with initdb, build schema, remove data directory options
		INITDB_FLAG=""
		if [[ ! -z "$INITDB" ]]; then
			INITDB_FLAG="-i"
		fi

		REMOVE_DIR_FLAG=""
		if [[ ! -z "$REMOVE_DATA_DIR" ]]; then
			REMOVE_DIR_FLAG="-z"
		fi

		CITUS_FLAG=""
		if [[ ! -z "$CITUS_COMPAT_MODE" ]]; then
			CITUS_FLAG="-c"
		fi

		# Only pass build schema on the first iteration when -O (build once) is set
		SCHEMA_FLAG="$BUILD_SCHEMA"
		if [[ ! -z "$BUILD_SCHEMA_ONCE" && $i -gt 1 ]]; then
			SCHEMA_FLAG=""
		fi

		if [[ ! -z "$PG_INIT_SQL" ]]; then
			$SCRIPT_DIR/$BENCHMARK_SCRIPT $INITDB_FLAG $SCHEMA_FLAG $REMOVE_DIR_FLAG $CITUS_FLAG -C $PG_CONFIG -t $data_pg_logs_dir -x $HAMMERDB_INSTALL_DIR -r "$PG_INIT_SQL" 2>&1 | tee $script_logfile
		else
			$SCRIPT_DIR/$BENCHMARK_SCRIPT $INITDB_FLAG $SCHEMA_FLAG $REMOVE_DIR_FLAG $CITUS_FLAG -C $PG_CONFIG -t $data_pg_logs_dir -x $HAMMERDB_INSTALL_DIR 2>&1 | tee $script_logfile
		fi
		retval="$?"

		if [[ $retval -ne 0 ]];
		then
			echo "Aborting due to error." >&2
			exit 1
		fi

		# Capture the results in a summary file for easier access
		grep "TEST RESULT :" $script_logfile >> $summary_file
	done
}

# Use pg_config and get PG version
get_pg_version()
{
	PG_VERSION=$($PG_CONFIG --version | cut -d' ' -f2)

    echo "PostgreSQL Version [$PG_VERSION]"
}

# Run Benchmark
run_benchmark()
{
    export PG_INITDB_OPTS="$PG_INITDB_OPTS_BASE"
    benchmark_type="PG-$PG_VERSION"

	if [[ ! -z "$PRELOAD_LIBRARY" ]];
	then
        export PG_INITDB_OPTS="$PG_INITDB_OPTS -c shared_preload_libraries='"$PRELOAD_LIBRARY"'"
        benchmark_type="$benchmark_type-$PRELOAD_LIBRARY"
	fi

	run_loop "$benchmark_type"
}

# Check options passed in.
while getopts "h b:cC:e:H:Ii:l:n:OPr:SZt:" OPTION
do
    case $OPTION in
        h)
            usage
            exit_script 1
            ;;

        b)
            BENCHMARK_TYPE=$OPTARG
            ;;

        c)
            CITUS_COMPAT_MODE="true"
            ;;

        C)
            PG_CONFIG=$OPTARG
            ;;

        e)
            PG_CONF_FILE=$OPTARG
            ;;

        H)
            HAMMERDB_INSTALL_DIR=$OPTARG
            ;;

        I)
            INITDB="true"
            ;;

        i)
            ITERATIONS=$OPTARG
            ;;

        l)
            PRELOAD_LIBRARY=$OPTARG
            ;;

        n)
            BENCHMARK_NAME=$OPTARG
            ;;

        O)
            BUILD_SCHEMA="-S"
            BUILD_SCHEMA_ONCE="true"
            ;;

        P)
            PREPARE_ONLY="true"
            ;;

        r)
            PG_INIT_SQL=$OPTARG
            ;;

        S)
            BUILD_SCHEMA="-S"
            ;;

        Z)
            REMOVE_DATA_DIR="true"
            ;;

        t)
            WORK_DIR=$OPTARG
            ;;

        ?)
            usage
            exit_script
            ;;
    esac
done

# Validate and update setup
validate_args

# Source the environment file(s)
source $PG_CONF_FILE

# Run sanity checks before proceeding
sanity_check

# Get the PG version
get_pg_version

# If prepare-only mode, build schema and exit
if [[ ! -z "$PREPARE_ONLY" ]]; then
    benchmark_type="PG-$PG_VERSION"
    if [[ ! -z "$PRELOAD_LIBRARY" ]]; then
        export PG_INITDB_OPTS="$PG_INITDB_OPTS_BASE -c shared_preload_libraries='"$PRELOAD_LIBRARY"'"
        benchmark_type="$benchmark_type-$PRELOAD_LIBRARY"
    fi
    prepare_only "$benchmark_type"
fi

# Run benchmarks
run_benchmark

# We're done...
echo
echo "Benchmarking completed!"

# Print summary results
echo
echo "RESULT SUMMARY"
echo "============================="
ls $WORK_DIR/$BENCHMARK_NAME.summary.*.log | xargs -I{} echo "echo {}; cat {}" | sh

# Perform clean up and exit.
exit_script 0
