puts "HammerDB CLI v5.0-test"

set configpg [dict create \
    connection [dict create pg_host "" pg_port "" pg_sslmode "" \
        pg_azure_citus false pg_citus_loadbalancer 7432 pg_citus_direct_workers true] \
    tpcc [dict create pg_dbase "" pg_defaultdbase "" pg_user "" pg_superuser "" \
        pg_pass old-secret pg_superuserpass old-secret pg_num_vu 1 pg_count_ware 1 \
        pg_cituscompat false pg_first_ware 1 pg_driver timed pg_rampup 0 \
        pg_duration 1 pg_vacuum true]]

if {[info exists ::env(TEST_NO_RANGE_SUPPORT)]} {
    dict unset configpg tpcc pg_first_ware
}
if {[info exists ::env(TEST_STOCK_HAMMERDB)]} {
    foreach key {pg_azure_citus pg_citus_loadbalancer pg_citus_direct_workers} {
        dict unset configpg connection $key
    }
    dict unset configpg tpcc pg_first_ware
}

proc dbset {args} {}
proc quotemeta {value} { return $value }
proc diset {section key value} {
    if {[string match *pass* $key]} {
        error "Passwords must not pass through the logging diset API"
    }
    dict set ::configpg $section $key $value
}
proc giset {args} {}
proc vuset {args} {}
proc loadscript {} {}
proc vucreate {} {}
proc vudestroy {} {}
proc vucomplete {} { return true }

proc run_phase {phase} {
    set trace [open $::env(TEST_TRACE) a]
    set location ""
    if {[info exists ::env(TEST_CURRENT_HOST)]} {
        set location "|host=$::env(TEST_CURRENT_HOST)|phase=$::env(TEST_CURRENT_PHASE)"
    }
    puts $trace "$phase|warehouses=$::env(HDB_WAREHOUSES)|first=$::env(HDB_FIRST_WAREHOUSE)|vus=$::env(HDB_RUN_VUS)$location"
    close $trace
    if {[dict get $::configpg tpcc pg_dbase] ne $::env(PGDATABASE) ||
        [dict get $::configpg tpcc pg_user] ne $::env(PGUSER)} {
        error "The adapter changed the canonical connection"
    }
    if {[dict get $::configpg tpcc pg_cituscompat] ne $::env(HDB_CITUS_COMPAT) ||
        ([dict exists $::configpg connection pg_azure_citus] &&
         [dict get $::configpg connection pg_azure_citus] ne $::env(HDB_CITUS_COMPAT))} {
        error "Citus compatibility did not reach the HammerDB dictionaries"
    }
    set expected_password ""
    # File-sourced credentials need an expectation independent of the environment.
    set expected_password_path "$::env(TEST_ROOT)/expected-password"
    if {[file exists $expected_password_path]} {
        set password_file [open $expected_password_path r]
        fconfigure $password_file -translation binary -encoding utf-8
        set expected_password [read $password_file]
        close $password_file
    } elseif {[info exists ::env(PGPASSWORD)]} {
        set expected_password $::env(PGPASSWORD)
    }
    if {[dict get $::configpg tpcc pg_pass] ne $expected_password ||
        [dict get $::configpg tpcc pg_superuserpass] ne $::env(HDB_SUPERUSER_PASSWORD)} {
        error "Credentials did not survive Tcl configuration"
    }
    if {[info exists ::env(TEST_EXPECT_EMPTY_ADMIN)] && $::env(TEST_EXPECT_EMPTY_ADMIN) &&
        $::env(HDB_SUPERUSER_PASSWORD) ne ""} {
        error "An explicitly empty admin password was replaced by a default"
    }
    if {[info exists ::env(TEST_HDB_FAIL)]} {
        puts stderr "Synthetic HammerDB failure"
        exit $::env(TEST_HDB_FAIL)
    }
    if {[info exists ::env(TEST_MISSING_COMPLETION)]} { exit 0 }
    if {[info exists ::env(TEST_BLOCK)]} {
        close [open "$::env(TEST_ROOT)/blocking" w]
        after 30000
    }
    set ::vustatus [dict create 1 "FINISH SUCCESS"]
    if {[info exists ::env(TEST_VU_FAIL)]} {
        dict set ::vustatus 1 "FINISH FAILED"
    }
    if {$phase eq "run" && ![info exists ::env(TEST_NO_METRICS)]} {
        puts "Vuser 1:TEST RESULT : System achieved 123.5 NOPM from 456 PostgreSQL TPM"
        if {[info exists ::env(TEST_DUPLICATE_METRICS)]} {
            puts "Vuser 1:TEST RESULT : System achieved 123.5 NOPM from 456 PostgreSQL TPM"
        }
    }
}
proc buildschema {} { run_phase prepare }
proc vurun {} { run_phase run }

source [lindex $argv 0]
