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


## USAGE
usage()
{
    errorCode=${1:-0}

    cat << EOF

usage: $0 OPTIONS

This script runs benchmarking for all of following permutations:
- PG
- Any preload shared library passed as an argument.

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
  -e  [PG_CONF_FILE]    PG configuration file.       [Default: $PG_CONF_FILE]
  -i  [ITERATIONS]      Users for benchmarking       [Default: $ITERATIONS]
  -l  [PRELOAD_LIBRARY] Users for benchmarking       [Default: $ITERATIONS]
  -n  [BENCHMARK_NAME]  Users for schemabuild        [Default: $BENCHMARK_NAME]

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
		$SCRIPT_DIR/$BENCHMARK_SCRIPT -i -S -z -C $PG_CONFIG -t $data_pg_logs_dir -x $HAMMERDB_INSTALL_DIR 2>&1 | tee $script_logfile
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
while getopts "h C:e:H:i:l:n:t:" OPTION
do
    case $OPTION in
        h)
            usage
            exit_script 1
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

        i)
            ITERATIONS=$OPTARG
            ;;

        l)
            PRELOAD_LIBRARY=$OPTARG
            ;;

        n)
            BENCHMARK_NAME=$OPTARG
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
if [[ -f $SCRIPT_DIR/$BENCHMARK_TYPE/$BENCHMARK_TYPE.env ]];
then
    source $SCRIPT_DIR/$BENCHMARK_TYPE/$BENCHMARK_TYPE.env
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
