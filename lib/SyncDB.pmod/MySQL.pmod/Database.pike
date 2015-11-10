inherit .TableManager;

#define SYNCDB_MIGRATION_DEBUG

function sqlcb;
string name;

array(this_program) other_databases() {
    if (!name) return ({});

    return .all_databases[name] - ({ this });
}

class TableVersion {
    inherit .SmartType;

    Field table_name = String(MAX_LENGTH(64), KEY);
    Field schema_version = Integer();
    Field type_versions = JSON(DEFAULT(([])));
    Field created = Datetime();
    Field migration_started = Datetime();
    Field migration_stopped = Datetime();
};

private object version_table;
constant version_table_name = "syncdb_versions";
// list of tables keeping references to table 'trigger'+'name'
mapping(string:mapping(string:array(function))) dependencies = ([]);

SyncDB.ReaderWriterLock rwlock;
Thread.Mutex mutex = Thread.Mutex();

function(int(0..1):void) maintenance_callback;

protected void create(function sqlcb, void|string name, void|function(int(0..1):void) maintenance_callback) {
    this_program::sqlcb = sqlcb;
    this_program::maintenance_callback = maintenance_callback;
    set_name(name);

    catch(has_version_table());
}

private mapping initiate_table_creation(string name, int schema_version, mapping type_versions) {
    //werror("initiate_table_creation(%O, %O, %O)\n", name, schema_version, type_versions);
    object sql = sqlcb();
    int now = time();
    // this throws an error if there is a collision on insert, due to the unique key table_name
    mixed err = catch {
        sql->query("INSERT INTO `syncdb_versions` "
                   "(table_name, schema_version, type_versions, created, migration_started) VALUES "
                   "(:table_name, :schema_version, :type_versions, :time, :time)",
                   ([
                    ":table_name" : name,
                    ":schema_version" : (string)schema_version,
                    ":type_versions" : Standards.JSON.encode(type_versions),
                    ":time" : format_datetime(now),
                    ]));
    };

    if (err) {
        if (sql->sqlstate && sql->sqlstate() == "23000") {
            return 0;
        } else {
            throw(err);
        }
    }

    return fetch_table_info(name);
}

private int parse_datetime(string|object s) {
    if (stringp(s)) {
        return Calendar.parse("%Y-%M-%D %h:%m:%s %z", s + " UTC")->unix_time();
    }

    return 0;
}

private string format_datetime(int ux) {
    return Calendar.Second("unix", ux)->set_timezone("UTC")->format_time();
}

private mapping decode_syncdb_version_entry(mapping m) {
    return ([
        "table_name" : m->table_name,
        "schema_version" : (int)m->schema_version,
        "type_versions" : Standards.JSON.decode(m->type_versions),
        "created" : parse_datetime(m->created),
        "migration_started" : parse_datetime(m->migration_started),
        "migration_stopped" : parse_datetime(m->migration_stopped),
        "version" : (int)m->version,
    ]);
}

private mapping fetch_table_info(string name) {
    object sql = sqlcb();

    array(mapping) tmp = sql->query("SELECT * from syncdb_versions where table_name = %s;", name);

    if (!sizeof(tmp)) return 0;
    return decode_syncdb_version_entry(tmp[0]);
}

array(mapping) fetch_running_migrations() {
    if (!has_version_table()) return 0;

    object sql = sqlcb();

    array(mapping) tmp = sql->query("SELECT * from syncdb_versions where migration_stopped is NULL;");

    return map(tmp, decode_syncdb_version_entry);
}

void stop_failed_migration(string name) {
    object sql = sqlcb();
    int now = time();
    sql->query("UPDATE syncdb_versions SET migration_stopped = %s, version = version + 1 "
               "WHERE table_name = %s AND migration_stopped is NULL",
               format_datetime(now), name);
}

private int start_table_migration(mapping v) {
    //werror("start_table_creation(%O)\n", v);
    object sql = sqlcb();
    int now = time();
    sql->query("UPDATE syncdb_versions "
               "SET migration_stopped = NULL, migration_started = %s, version = version + 1 "
               "WHERE table_name = %s AND version = %s",
               format_datetime(now), v->table_name, (string)v->version);
    int affected = sql->master_sql->affected_rows(); 
    if (affected) {
        v->migration_started = now;
        v->migration_stopped = 0;
        v->version ++;
    }
    return affected;
}

private int stop_table_migration(mapping v) {
    //werror("stop_table_creation(%O)\n", v);
    object sql = sqlcb();
    int now = time();
    sql->query("UPDATE syncdb_versions SET migration_stopped = %s, version = version + 1 "
               "WHERE table_name = %s AND version = %s",
               format_datetime(now), v->table_name, (string)v->version);
    int affected = sql->master_sql->affected_rows(); 
    if (affected) {
        v->migration_stopped = now;
        v->version ++;
    }
    return affected;
}

private int finish_table_migration(mapping v, int schema_version, mapping type_versions) {
    //werror("finish_table_creation(%O, %O, %O)\n", v, schema_version, type_versions);
    object sql = sqlcb();
    int now = time();
    sql->query("UPDATE syncdb_versions "
               "SET migration_stopped = %s, schema_version = %s, type_versions = %s, version = version + 1 "
               "WHERE table_name = %s AND version = %s",
               format_datetime(now), (string)schema_version, Standards.JSON.encode(type_versions),
               v->table_name, (string)v->version);
    int affected = sql->master_sql->affected_rows(); 
    if (affected) {
        v->migration_stopped = now;
        v->version ++;
        v->type_versions = type_versions;
        v->schema_version = schema_version;
    }
    return affected;
}

void set_name(string name) {
    if (this_program::name) {
        .unregister_database(this_program::name, this);
    }

    this_program::name = name;

    if (name) {
        rwlock = .register_database(name, this);
    } else {
        rwlock = SyncDB.ReaderWriterLock();
    }
}

Thread.MutexKey get_database_key() {
    return rwlock->lock_read();
}

Thread.MutexKey try_get_database_key() {
    return rwlock->try_lock_read();
}

Thread.MutexKey get_database_key_or_callback(function f, mixed ... args) {
    return rwlock->lock_read_or_callback(f, @args);
}

array(function) get_maintenance_callbacks() {
    array dbs = name ? .all_databases[name] : ({ this });
    dbs = dbs->maintenance_callback;
    dbs = filter(dbs, dbs);
    if (sizeof(dbs)) return dbs;
    return 0;
}

void call_maintenance_callbacks(int(0..1) maintenance) {
    array(function) maintenance_cbs = get_maintenance_callbacks();

    if (maintenance_cbs) {
        foreach (maintenance_cbs;; function f) {
            mixed err = catch(f(maintenance));
            if (err) master()->handle_error(err);
        }
    }
}

SyncDB.ReaderWriterLockKey get_maintenance_key(void|int(0..1) signal) {
    SyncDB.ReaderWriterLockKey key = rwlock->lock_write();

    // we invalidate the version table here, assuming that maintenance might
    // involve db restore, etc
    version_table = 0;
    
    if (signal) {
        call_maintenance_callbacks(1);

        key->done_cb = Function.curry(call_maintenance_callbacks)(0);
    }

    return key;
}

void call_with_database_key(function f, mixed ... args) {
    rwlock->call_with_read_key(f, @args);
}

int(0..1) has_version_table() {
    Sql.Sql sql = sqlcb();

    if (!sql) error("Cannot connect to SQL server.\n");

    int has = has_value(sql->list_tables(version_table_name), version_table_name);

    if (!has) return 0;

    Thread.MutexKey key = mutex->lock();

    mapping info = fetch_table_info(version_table_name);

    if (!info) {
        .remove_version_triggers(sql, version_table_name);
        object schema = TableVersion()->schema;
        info = initiate_table_creation(version_table_name, schema->get_schema_version(),
                                            schema->type_versions());

        if (info) {
            object mig = SyncDB.Migration.Base(schema->get_previous_schema(0, ([])), schema);
            mig->upgrade_table(version_table_name)(sql);

            stop_table_migration(info);
        } else while (info && !info->migration_stopped) {
            info = fetch_table_info(version_table_name);
            sleep(0.5);
        }
    }

    return 1;
}

object get_version_table() {
    if (version_table) return version_table;
    Thread.MutexKey key = mutex->lock();
    if (version_table) return version_table;
    if (!has_version_table()) return 0;
    version_table = TableVersion()->get_table(sqlcb, version_table_name);
    //register_table(version_table_name, version_table);
    return version_table;
}

void create_version_table() {
    object schema = TableVersion()->schema;
    SyncDB.Migration.Base(0, schema)->create_table(version_table_name)(sqlcb());

    mapping v = initiate_table_creation(version_table_name, schema->get_schema_version(),
                                        schema->type_versions());
    if (v) stop_table_migration(v);
}

void destroy() {
    if (name) .unregister_database(name, this);
    if (version_table) unregister_table(version_table_name, version_table);
}

//! register a trigger from a remote table
void register_dependency(string table, string trigger, function fun) {
    Thread.MutexKey key = mutex->lock();
    if (!dependencies[table])
        dependencies[table] = ([]);

    if (!dependencies[table][trigger])
        dependencies[table][trigger] = ({});

    dependencies[table][trigger] += ({ fun });
}

void register_trigger(string table, string trigger, function fun) {
    register_dependency(table, trigger, fun);
}

void unregister_dependency(string table, string trigger, function fun) {
    Thread.MutexKey key = mutex->lock();
    dependencies[table][trigger] -= ({ fun });
}

void unregister_trigger(string table, string trigger, function fun) {
    unregister_dependency(table, trigger, fun);
}

Thread.Mutex migration_mutex = Thread.Mutex();


object get_table(string name, void|object|program type) {
    if (objectp(type)) {
        return low_get_table(name, type) || low_get_table(name, object_program(type)) || ::get_table(name, type);
    }

    return ::get_table(name, type);
}

object register_view(string name, object type) {
    object table;

    object key = migration_mutex->lock();

    if ((table = low_get_table(name, type)) ||
        (table = low_get_table(name, object_program(type)))) {
        return table;
    }

    destruct(key);

    Sql.Sql sql = sqlcb();

    int vtable = has_version_table();

    if (vtable) {
        object schema = type->schema;
        mapping type_versions = schema->type_versions();
        int schema_version = schema->get_schema_version();

RETRY: do {
            mapping v = fetch_table_info(name);
            array(object) migrations = ({ });

            if (!v) {
                // does the table even exist?
                if (!has_value(sql->list_tables(name), name)) {
                    v = initiate_table_creation(name, schema_version, type_versions);

                    if (v) {
                        SyncDB.Migration.Base(0, schema)->create_table(name)(sql);
                        if (stop_table_migration(v)) {
                            break RETRY;
                        } else error("Could not stop migration %O.\n", v);
                    }
                    continue;
                } else {
                    v = initiate_table_creation(name, -1, ([]));

                    if (!v) {
                        continue;
                    }
                }
            } else {
                if (!v->migration_stopped) {
                    int since = time() - v->migration_started;
                    // 5 minutes seems fair?
                    if (since > 5 * 60)
                        error("A Migration has been running since %d seconds ago. Probably died. Fix manually!\n", since);
#ifdef SYNCDB_MIGRATION_DEBUG
                    //werror("Observing a migration in flight on %O. wait for it.\n", name);
#endif
                    sleep(0.5);
                    continue;
                }
            }

            int current_schema_version = v->schema_version;
            mapping current_type_versions = v->type_versions;

            // nothing to do.
            if (schema_version == current_schema_version &&
                equal(type_versions, current_type_versions)) break;

            
            int fail;

            v = fetch_table_info(name);

            // we have locked the table, v cannot change anymore
            current_schema_version = v->schema_version;
            current_type_versions = v->type_versions;

            // nothing to do.
            if (schema_version == current_schema_version &&
                equal(type_versions, current_type_versions)) {
                break;
            }

            if (!v->migration_stopped) {
                // there is another migration running, which is not done yet
                continue;
            }

            if (!start_table_migration(v)) {
                // we got raced
                continue;
            }

            if (current_schema_version == -1) {
                object initial_schema = schema->get_previous_schema(0, ([]));
                migrations += ({
                    SyncDB.Migration.Simple(initial_schema, initial_schema)
                });
            } else if (schema_version != current_schema_version ||
                       !equal(type_versions, current_type_versions)) {
                if (schema_version < v->schema_version)
                    error("Requested Schema version older than database.\n");

                migrations += schema->get_migrations(current_schema_version, current_type_versions);
            }

            if (sizeof(migrations)) {
                //werror("---- %s ----\ncurrent version: %d\ncurrent types: %O\n-----\n",
                //       name, current_schema_version, current_type_versions);
                int t1 = gethrtime();
#ifdef SYNCDB_MIGRATION_DEBUG
                werror("Table(%O): %s\n", name, migrations[0]->describe());
#endif

                mixed err = catch {
                    migrations[0]->migrate(sql, name);
                };

                if (err) {
                    stop_table_migration(v);
                    throw(err);
                }

#ifdef SYNCDB_MIGRATION_DEBUG
                werror("Table(%O): DONE in %f seconds\n", name, (gethrtime() - t1)/1E6);
#endif

                fail = !finish_table_migration(v, 
                                               migrations[0]->to->get_schema_version(),
                                               migrations[0]->to->type_versions());
            } else {
                // nothing at all to do. for instance for "empty" tables
                fail = !finish_table_migration(v,
                                               schema->get_schema_version(),
                                               schema->type_versions());
            }


            if (fail) {
                error("Could not register the migration end.\n");
            }
        } while (1);
    }

    // all the above is meant to run in parallel

    key = migration_mutex->lock();

    if (table = low_get_table(name, type)) return table;

    if (vtable) {
        table = type->get_table(sqlcb, name);
    } else {
        table = type->get_previous_table(sqlcb, name, 0, ([]));
    }

    register_table(name, table);

    return table;
}

private void _remote_trigger(string event, object table, mixed ... args) {
    string table_name = table->table_name();
    array(function) triggers = dependencies[?table_name][?event];

    table = low_get_table(table_name);

    if (table && triggers) foreach (triggers;; function f) {
        mixed err = catch(f(table, @args));
        if (err) master()->handle_error(err);
    }
}

function get_remote_trigger(string table_name, string event) {
    if (dependencies[?table_name][?event]) {
        return Function.curry(_remote_trigger)(event);
    } else return 0;
}

array(function) get_triggers(string table, string event) {
    // we do not allow before_ events for external dbs, and in particular we dont let them
    // cancel events in other dbs
    array ret = ({});

    foreach (({ table, 0 });; string table_name) {
        if (name && has_prefix(event, "after_")) {
            // this db is registered
            array dbs = .all_databases[name];
            if (arrayp(dbs)) {
                dbs -= ({ this });
                dbs = filter(dbs->get_remote_trigger(table_name, event), functionp);
                ret += dbs;
            }
        }
        ret += dependencies[?table_name][?event] || ({ });
    }
    return ret;
}

void unregister_view(string name, object type) {
    object table = low_get_table(name, type);
    if (table) {
        unregister_table(name, table);
    } else {
        werror("unregistering unknown table for %O %O\n", name, type);
    }
}

void register_table(string name, object table) {
    Thread.MutexKey key = mutex->lock();
    ::register_table(name, table);
    destruct(key);
    table->set_database(this);
}

void unregister_table(string name, object table) {
    Thread.MutexKey key = mutex->lock();
    if (!::unregister_table(name, table)) {
        error("%O is not registered here.\n", table);
    }
    destruct(key);
    table->set_database();
    destruct(table);
}
