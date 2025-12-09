#!/bin/bash

# SCRIPT: pg_pg_benchmark.sh
#-----------------------------
# See usage on how to run this script.
#

set -o pipefail

## GLOBAL VARIABLES

# Paths and file names
export SCRIPT_WORKING_DIR=""
export HAMMERDB_INSTALL_DIR=""
export SCRIPT_BULID_SCHEMA_TCL="pg_tpcc_schemabuild.tcl"
export SCRIPT_BENCHMARK_TCL="pg_tpcc_benchmark.tcl"

# Internal bools for identifying operations and cleanups
export SHOULD_INITDB=0
export SHOULD_BUILDSCHEMA=0
export IS_SERVER_STARTED=0
export SHOULD_CLEAN_DATA=0

# PostgreSQL and HammerDB configuration
export PG_CONFIG="${PG_CONFIG}"
export PGHOST="${PGHOST:-localhost}"
export PGPORT="${PGPORT:-5432}"

export PG_DBASE="${PG_DBASE:-tpcc}"
export PG_DEFAULTDBASE="${PG_DEFAULTDBASE:-postgres}"
export PG_SUPERUSER="${PG_SUPERUSER:-postgres}"
export PG_USER="${PG_USER:-tpcc}"

export PG_COUNT_WARE="${PG_COUNT_WARE:-20}"
export PG_DURATION="${PG_DURATION:-5}"
export PG_RAMPUP="${PG_RAMPUP:-2}"
export PG_NUM_VU="${PG_NUM_VU:-20}"
export PG_VU="${PG_VU:-20}"

# Set a default data directory if we need to perform initdb
export DATA_DIR_NAME="data.tpcc"

# SQL script to run after initdb (optional)
export PG_INIT_SQL=""


## USAGE
usage()
{
    errorCode=${1:-0}

    cat << EOF

usage: $0 OPTIONS

This script can perform:
- initdb
- Start PostgreSQL server
- Generate build schema tcl script and build schema
- Generate run tpcc benchmark script and run benchmark

Some of the options can also be set via environment variables. The relevant
variable is provided with each option. However, some arguments are mandatory
to prevent any accidental environment/data corruption.

OPTIONS can be:

  -h  Show this message

  -C  [PG_CONFIG]         pg_config path            [REQUIRED]
  -H  [PGHOST]            Host                      [Default: $PGHOST]
  -p  [PGPORT]            Port                      [Default: $PGPORT]

  -i                      Initialize data directory $DATA_DIR_NAME at path
                          specificed by "-t" option.
                          
                          * PG_INITDB_OPTS environment variable is passed to
                            set PostgreSQL at start time.
  -S                      Build tpcc schema


  -b  [PG_DBASE]          Benchmarking database     [Default: $PG_DBASE]
  -d  [PG_DEFAULTDBASE]   Default database          [Default: $PG_DEFAULTDBASE]
  -D  [PG_DURATION]       Benchmark duration in min [Default: $PG_DURATION]
  -s  [PG_SUPERUSER]      Superuser                 [Default: $PG_SUPERUSER]
  -v  [PG_USER]           Benchmark user            [Default: $PG_USER]
  -w  [PG_COUNT_WARE]     Number of warehouses      [Default: $PG_COUNT_WARE]
  -u  [PG_NUM_VU]         Users for schemabuild     [Default: $PG_NUM_VU]
  -U  [PG_VU]             Users for benchmarking    [Default: $PG_VU]

  -t                      Script working folder     [REQUIRED]
                          * a folder where data directory and relevant log
						    files may be created
  -x                      HammerDB installation dir [REQUIRED]

  -r  [PG_INIT_SQL]       SQL script to run after initdb [Default: none]

  -z                      Remove data directory on exit.
                          * this saves space and allows multiple iterations of
                            benchmarking while preserving logs.

EOF

    if [[ $errorCode -ne 0 ]];
    then
        exit_script $errorCode
    fi
}

# Perform any required cleanup and exit with the given error/success code
exit_script()
{
    if [[ $IS_SERVER_STARTED -eq 1 ]];
    then
        postgresql_stop
    fi

    if [[ $SHOULD_CLEAN_DATA -eq 1 ]];
    then
        echo "Removing data directory $SCRIPT_WORKING_DIR/$DATA_DIR_NAME"
        rm -rf $SCRIPT_WORKING_DIR/$DATA_DIR_NAME
    fi

    # Exit with a given return code or 0 if none are provided.
    exit ${1:-0}
}

# Vaildate arguments to ensure that we can safely run the benchmark
validate_args()
{
    if [[ ! -f "$PG_CONFIG" ]];
    then
        echo "pg_config pathname is required. See usage for details" >&2
        usage 1
    fi

    if [[ ! -d "$SCRIPT_WORKING_DIR" ]];
    then
        echo "Script working directory for the script is required. See usage for details" >&2
        usage 1
    fi

    if [[ ! -d "$HAMMERDB_INSTALL_DIR" ]];
    then
        echo "Incorrect path for hammerdb installation. See usage for details" >&2
        usage 1
    fi
}

# Update environment variables and paths for benchmarking
set_environment()
{
    retval=0

    export LANG=C
    export LC_ALL=C
    export LG_LANG=C

    export PATH=$($PG_CONFIG --bindir):$PATH
    export LD_LIBRARY_PATH=$($PG_CONFIG --libdir):$LD_LIBRARY_PATH

    # Create working directory if it doesn't already exist
    echo "Creating script working folder: $SCRIPT_WORKING_DIR"
    mkdir -p $SCRIPT_WORKING_DIR
    retval=$?

    if [[ $retval -ne 0 ]];
    then
        echo "Unable to create working folder: $SCRIPT_WORKING_DIR" >&2
        exit_script 1
    fi
}

# Perform initdb
postgresql_initialize()
{
    retval=0

    initdb -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME 2>&1 | tee $SCRIPT_WORKING_DIR/initdb.log
    retval=$?

    if [[ $retval -ne 0 ]];
    then
        echo "initdb failed. See log file [$SCRIPT_WORKING_DIR/initdb.log] for details." >&2
        exit_script 1
    fi
}

# Start postgresql server
postgresql_start()
{
    retval=0

    # If options are specificed, set those
    if [[ ! -z "$PG_INITDB_OPTS" ]];
    then
        echo "Starting server as: pg_ctl -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME -l $SCRIPT_WORKING_DIR/server.log -o \"$PG_INITDB_OPTS\""
        pg_ctl -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME -l $SCRIPT_WORKING_DIR/server.log -o "$PG_INITDB_OPTS" start
        retval=$?
    else
        echo "Starting server as: pg_ctl -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME -l $SCRIPT_WORKING_DIR/server.log"
        pg_ctl -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME -l $SCRIPT_WORKING_DIR/server.log start
        retval=$?
    fi

    # Check if the server started successfully
    if [[ $retval -ne 0 ]];
    then
        echo "pg_ctl start failed. See log file $SCRIPT_WORKING_DIR/server.log for details." >&2
        exit_script 1
    fi

    # Set this to 1 so that we know to shutdown the server before exiting
    IS_SERVER_STARTED=1

    # Wait for the server to accept connections
    echo -n "Waiting for server to be ready to accept connections..."

    # Wait for 150 seconds
    for i in {1..30};
    do
        pg_isready -q -t 5
        if [[ $? -eq 0 ]];
        then
            echo " done!"
            break;
        fi

        echo $i
    done

    # Check if all good and we are proceed on with the benchmarking process
    pg_isready -q
    if [[ $? -ne 0 ]];
    then
        echo " unable to verify. See log file $SCRIPT_WORKING_DIR/server.log for details."
    fi

    # Does the super user already exist?
    superuserExists=$(psql postgres -tAc "SELECT count(0) FROM pg_roles WHERE rolname='"$PG_SUPERUSER"'")

    # Let's create if not
    if [[ $superuserExists -eq 0 ]];
    then
        echo "Creating superuser $PG_SUPERUSER"
        createuser -d -s $PG_SUPERUSER
    fi
}

# Run SQL script after initdb if specified
postgresql_run_init_sql()
{
    retval=0

    if [[ ! -z "$PG_INIT_SQL" ]];
    then
        if [[ ! -f "$PG_INIT_SQL" ]];
        then
            echo "SQL script file does not exist: $PG_INIT_SQL" >&2
            exit_script 1
        fi

        echo "Running SQL script: $PG_INIT_SQL"
        psql -h $PGHOST -p $PGPORT -U $PG_SUPERUSER -d postgres -f "$PG_INIT_SQL" 2>&1 | tee $SCRIPT_WORKING_DIR/init_sql.log
        retval=$?

        if [[ $retval -ne 0 ]];
        then
            echo "SQL script execution failed. See log file [$SCRIPT_WORKING_DIR/init_sql.log] for details." >&2
            exit_script 1
        fi

        echo "SQL script executed successfully."
    fi
}

# Stop postgresql server
postgresql_stop()
{
    retval=0

    # Stop immediate
    pg_ctl -D $SCRIPT_WORKING_DIR/$DATA_DIR_NAME -l $SCRIPT_WORKING_DIR/server.log -m immediate stop
    retval=$?

    if [[ $retval -ne 0 ]];
    then
        echo "pg_ctl stop failed. See log file [$SCRIPT_WORKING_DIR/server.log] for details." >&2

        # Not calling exit_script to avoid infinite recursion and preserve data and logs for analysis
        exit 1
    fi

    IS_SERVER_STARTED=0
}

# Restart postgresql server
postgresql_restart()
{
    postgresql_stop

    # Let's wait a bit
    sleep 30

    postgresql_start
}

# Generate tcl file under $SCRIPT_WORKING_DIR for building schema
tpcc_build_schema_gen_script()
{
    echo "Creating build schema script: $SCRIPT_WORKING_DIR/$SCRIPT_BULID_SCHEMA_TCL"

    cat << EOF >> $SCRIPT_WORKING_DIR/$SCRIPT_BULID_SCHEMA_TCL
#!/bin/tclsh

puts "SETTING CONFIGURATION"

dbset db pg
diset connection pg_host $PGHOST
diset connection pg_port $PGPORT

diset tpcc pg_dbase $PG_DBASE
diset tpcc pg_defaultdbase $PG_DEFAULTDBASE
diset tpcc pg_user $PG_USER
diset tpcc pg_superuser $PG_SUPERUSER
diset tpcc pg_num_vu $PG_NUM_VU
diset tpcc pg_count_ware $PG_COUNT_WARE

print dict

buildschema
waittocomplete

puts "BUILD SCHEMA COMPLETE"
quit
EOF
}

# Generate tcl file under $SCRIPT_WORKING_DIR for running the benchmark.
#   - The script also has a timer so that we'd know when to stop the hammerdbcli.
#     The timer is calculated based rampup time, benchmark duration and a 3 minute
#     additional buffer.
tpcc_run_gen_script()
{
    # add 3 minutes to the timer
    wait_timer=$(expr $PG_DURATION \* 60 + 180)

    echo "Creating benchmark script: $SCRIPT_WORKING_DIR/$SCRIPT_BENCHMARK_TCL"

    cat << EOF >> $SCRIPT_WORKING_DIR/$SCRIPT_BENCHMARK_TCL
#!/bin/tclsh

puts "SETTING CONFIGURATION"

dbset db pg
diset connection pg_host $PGHOST
diset connection pg_port $PGPORT

diset tpcc pg_dbase $PG_DBASE
diset tpcc pg_defaultdbase $PG_DEFAULTDBASE
diset tpcc pg_user $PG_USER
diset tpcc pg_superuser $PG_SUPERUSER
diset tpcc pg_num_vu $PG_NUM_VU
diset tpcc pg_count_ware $PG_COUNT_WARE

diset tpcc pg_driver timed
diset tpcc pg_rampup $PG_RAMPUP
diset tpcc pg_duration $PG_DURATION
diset tpcc pg_vacuum true
vuset logtotemp 1

loadscript
vuset vu $PG_VU

print dict

puts "BENCHMARK STARTED"
vucreate
vurun
runtimer $wait_timer
vudestroy
after $wait_timer
puts "BENCHMARK COMPLETE"

EOF
}

# Build schema for TPCC workload using $SCRIPT_WORKING_DIR/$SCRIPT_BULID_SCHEMA_TCL file.
# - This file is automatically generated tpcc_build_schema_gen_script function.
tpcc_build_schema()
{
    hammerdb_run_script "$SCRIPT_WORKING_DIR/$SCRIPT_BULID_SCHEMA_TCL"
}

# Run TPCC benchmark using $SCRIPT_WORKING_DIR/$SCRIPT_BENCHMARK_TCL file.
# - This file is automatically generated tpcc_run_gen_script function.
tpcc_run_benchmark()
{
    echo "Database size is: $(du -sh $SCRIPT_WORKING_DIR/$DATA_DIR_NAME)"
    hammerdb_run_script "$SCRIPT_WORKING_DIR/$SCRIPT_BENCHMARK_TCL"
}

# Internal function for running a script with hammerdbcli.
hammerdb_run_script()
{
    pushd $HAMMERDB_INSTALL_DIR
    ./hammerdbcli auto "$1"
    popd
}

# Check options passed in to the script.
while getopts "h iSz C:H:p: b:d:D:s:v:w:u:U:r: t:x:" OPTION
do
    case $OPTION in
        h)
            usage
            exit_script 1
            ;;

        C)
            PG_CONFIG=$OPTARG
            ;;
        H)
            PGHOST=$OPTARG
            ;;
        p)
            PGPORT=$OPTARG
            ;;

        i)
            SHOULD_INITDB=1
            ;;
        S)
            SHOULD_BUILDSCHEMA=1
            ;;
        z)
            SHOULD_CLEAN_DATA=1
            ;;

        b)
            PG_DBASE=$OPTARG
            ;;
        d)
            PG_DEFAULTDBASE=$OPTARG
            ;;
        D)
            PG_DURATION=$OPTARG
            ;;
        s)
            PG_SUPERUSER=$OPTARG
            ;;
        v)
            PG_USER=$OPTARG
            ;;
        w)
            PG_COUNT_WARE=$OPTARG
            ;;
        u)
            PG_NUM_VU=$OPTARG
            ;;
        u)
            PG_NUM_VU=$OPTARG
            ;;
        U)
            PG_VU=$OPTARG
            ;;

        r)
            PG_INIT_SQL=$OPTARG
            ;;

        t)
            SCRIPT_WORKING_DIR=$OPTARG
            ;;
        x)
            HAMMERDB_INSTALL_DIR=$OPTARG
            ;;

        ?)
            usage
            exit_script
            ;;
    esac
done

# Validate and update setup
validate_args
set_environment

# Initialize the databse and start the server if asked to do so via 
# commandline argument
if [[ $SHOULD_INITDB -eq 1 ]];
then
    postgresql_initialize
    postgresql_start
    postgresql_run_init_sql
fi

# Build schema if asked to do so via commandline argument
if [[ $SHOULD_BUILDSCHEMA -eq 1 ]];
then
    tpcc_build_schema_gen_script
    tpcc_build_schema
fi

# Generate script and run benchmark
tpcc_run_gen_script
tpcc_run_benchmark

# Stop the server if we started it.
if [[ $SHOULD_INITDB -eq 1 ]];
then
    postgresql_stop
fi

# Perform clean up and exit.
exit_script 0