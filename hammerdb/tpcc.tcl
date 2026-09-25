# Run inside HammerDB with configuration supplied by the adapter environment.
proc configure_setting {section key value} {
    if {![dict exists $::configpg $section $key]} {
        error "This HammerDB build does not support $section:$key"
    }
    diset $section $key $value
}

proc configure_password {key environment_name} {
    if {![dict exists $::configpg tpcc $key]} {
        error "This HammerDB build does not support tpcc:$key"
    }
    set password ""
    if {[info exists ::env($environment_name)]} {
        set password $::env($environment_name)
    }
    # diset prints old/new values and persists them; keep credentials in memory.
    dict set ::configpg tpcc $key [quotemeta $password]
}

proc verify_virtual_users {} {
    if {![info exists ::vustatus] || [dict size $::vustatus] == 0} {
        error "HammerDB did not start any virtual users"
    }
    # Service Tcl events while waiting for the virtual users to finish.
    while {![vucomplete]} {
        after 100
        update
    }
    dict for {user status} $::vustatus {
        if {$status ne "FINISH SUCCESS"} {
            error "Virtual user $user did not succeed: $status"
        }
    }
}

# Convert Tcl failures into a nonzero exit for the supervising runner.
if {[catch {
    dbset db pg
    dbset bm TPC-C
    configure_setting connection pg_host $::env(PGHOST)
    configure_setting connection pg_port $::env(PGPORT)
    configure_setting connection pg_sslmode $::env(PGSSLMODE)
    configure_setting tpcc pg_dbase $::env(PGDATABASE)
    configure_setting tpcc pg_defaultdbase $::env(PGMAINTENANCE_DB)
    configure_setting tpcc pg_user $::env(PGUSER)
    configure_setting tpcc pg_superuser $::env(HDB_SUPERUSER)
    configure_password pg_pass PGPASSWORD
    configure_password pg_superuserpass HDB_SUPERUSER_PASSWORD
    configure_setting tpcc pg_num_vu $::env(HDB_BUILD_VUS)
    configure_setting tpcc pg_count_ware $::env(HDB_WAREHOUSES)
    configure_setting tpcc pg_cituscompat $::env(HDB_CITUS_COMPAT)

    # These build-specific keys are mandatory only when Citus mode is enabled.
    foreach {key value} [list \
        pg_azure_citus $::env(HDB_CITUS_COMPAT) \
        pg_citus_loadbalancer $::env(HDB_CITUS_LOADBALANCER_PORT) \
        pg_citus_direct_workers $::env(HDB_CITUS_DIRECT_WORKERS)] {
        if {[dict exists $::configpg connection $key]} {
            configure_setting connection $key $value
        } elseif {$::env(HDB_CITUS_COMPAT)} {
            error "Citus mode requires a HammerDB build supporting connection:$key"
        }
    }

    if {[dict exists $::configpg tpcc pg_first_ware]} {
        configure_setting tpcc pg_first_ware $::env(HDB_FIRST_WAREHOUSE)
    } elseif {$::env(HDB_DISTRIBUTED_LOAD)} {
        error "Distributed loading requires a HammerDB build supporting pg_first_ware and the zero-warehouse DDL phase"
    }

    if {$::env(HDB_PHASE) eq "prepare"} {
        giset virtual_user_options virtual_users $::env(HDB_BUILD_VUS)
        giset virtual_user_options user_delay 1
        vuset delay 1
        buildschema
        verify_virtual_users
        vudestroy
        puts "BENCHMARK_PREPARE_COMPLETE"
    } elseif {$::env(HDB_PHASE) eq "run"} {
        configure_setting tpcc pg_driver timed
        configure_setting tpcc pg_rampup $::env(HDB_RAMPUP_MINUTES)
        configure_setting tpcc pg_duration $::env(HDB_DURATION_MINUTES)
        configure_setting tpcc pg_vacuum $::env(HDB_VACUUM)
        giset virtual_user_options virtual_users $::env(HDB_RUN_VUS)
        giset virtual_user_options user_delay 1
        vuset delay 1
        vuset logtotemp 1
        loadscript
        vuset vu $::env(HDB_RUN_VUS)
        vucreate
        vurun
        verify_virtual_users
        vudestroy
        puts "BENCHMARK_RUN_COMPLETE"
    } else {
        error "Unknown HammerDB phase: $::env(HDB_PHASE)"
    }
} message]} {
    puts stderr "BENCHMARK ERROR: $message"
    exit 1
}
exit 0
