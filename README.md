The benchmarking process with HammerDB is automated. wrapper.sh is the script to use. The script requires:
- PostgreSQL installation
- Any designed extension already installed; e.g. pg_stat_statements, pg_stat_monitor, etc., and
- HammerDB installation.

This is the first version of the scripts. The 3 script files are:
- **hammerdb.sh**: 
Contains the code for performing initdb, starting the server, and performing benchmark using HammerDB.

- **pg.env**: 
Contains configuration for PG

- **wrapper.sh**: 
The script sources pg.env file as well as <benchmark>.env file. In case of HammerDB, this transalates to hammerdb/hammerdb.env file. It has the ability to run multiple iterations of the benchmarks. See the envinronment file hammerdb/hammerdb.env and hammerdb/hammerdb.sh for benchmark specific variables.


### Running the Script

You can easily benchmark PostgreSQL with or without any extensions by running the wrapper.sh script. It requires three mandatory commandline arguments:
- Path to pg_config
- Path to HammerDB installation directory
- Working folder for the script where it'll create data directory and relevant log files.

For example:

`$ ./wrapper.sh -C /home/vagrant/postgres.14/inst/bin/pg_config -H /home/vagrant/HammerDB-4.4 -t /tmp/xyz`

### Script Output

Under the working directory, the script will create 
- PG-<VERSION> folder that contains server, initdb log files along with data directory.
- One log for each iteration of benchmarking
- A summary log file that contains one line summary of each benchmark iteration.

For example, the summary file may look something like:

```
Vuser 1:TEST RESULT : System achieved 42332 NOPM from 97427 PostgreSQL TPM
Vuser 1:TEST RESULT : System achieved 38385 NOPM from 88486 PostgreSQL TPM
Vuser 1:TEST RESULT : System achieved 37331 NOPM from 86015 PostgreSQL TPM
```