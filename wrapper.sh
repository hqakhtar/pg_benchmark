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
export BUILD_SCHEMA="true"
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

  -C  [PG_CONFIG]       pg_config path               [REQUIRED]
  -H                    HammerDB installation dir    [REQUIRED]
  -t                    Script working folder        [REQUIRED]
                        * a folder where data directory and relevant log
                          files may be created.

  -b  [BENCHMARK_Type]  Type of benchmark to run     [Default: $BENCHMARK_TYPE]
  -c                    Enable Citus compatibility   [Default: not set]
  -e  [PG_CONF_FILE]    PG configuration file.       [Default: $PG_CONF_FILE]
  -I                    Run initdb                   [Default: not set]
  -i  [ITERATIONS]      Number of iterations         [Default: $ITERATIONS]
  -l  [PRELOAD_LIBRARY] Shared preload library       [Default: none]
  -n  [BENCHMARK_NAME]  Benchmark name               [Default: $BENCHMARK_NAME]
  -r  [PG_INIT_SQL]     SQL script to run after initdb [Default: none]
  -S                    Build schema                 [Default: set]
  -Z                    Remove data directory        [Default: not set]

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

		SCHEMA_FLAG=""
		if [[ ! -z "$BUILD_SCHEMA" ]]; then
			SCHEMA_FLAG="-S"
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
while getopts "h b:cC:e:H:Ii:l:n:r:SZt:" OPTION
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

        r)
            PG_INIT_SQL=$OPTARG
            ;;

        S)
            BUILD_SCHEMA="true"
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

# When running against existing cluster (no initdb), limit iterations to 1
if [[ -z "$INITDB" ]]; then
    if [[ $ITERATIONS -gt 1 ]]; then
        echo "WARNING: Running against existing cluster. Limiting iterations to 1 (was set to $ITERATIONS)." >&2
        ITERATIONS=1
    fi
    # Ensure data directory is not cleaned up when running against existing cluster
    if [[ ! -z "$REMOVE_DATA_DIR" ]]; then
        echo "WARNING: -Z flag ignored when running against existing cluster." >&2
        REMOVE_DATA_DIR=""
    fi
fi

# Get the PG version
get_pg_version

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
