inherit .TableManager;

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

void create(function sqlcb, void|string name) {
    this_program::sqlcb = sqlcb;
    this_program::name = name;
    if (name) .register_database(name, this);
}

int(0..1) has_version_table() {
    Sql.Sql sql = sqlcb();

    return sql && has_value(sql->list_tables(version_table_name), version_table_name);
}

Thread.Mutex mutex = Thread.Mutex();

object get_version_table() {
    if (version_table) return version_table;
    Thread.MutexKey key = mutex->lock();
    if (version_table) return version_table;
    if (!has_version_table()) return 0;
    version_table = TableVersion()->get_table(sqlcb, version_table_name);
    register_table(version_table_name, version_table);
    return version_table;
}

void create_version_table() {
    SyncDB.Migration.Base(0, TableVersion()->schema)->create_table(version_table_name)(sqlcb());
}

void destroy() {
    if (name) .unregister_database(name, this);
    if (version_table) unregister_table(version_table_name, version_table);
}

//! register a trigger from a remote table
void register_dependency(string table, string trigger, function fun) {
    if (!dependencies[table])
        dependencies[table] = ([]);

    if (!dependencies[table][trigger])
        dependencies[table][trigger] = ({});

    dependencies[table][trigger] += ({ fun });
}

void unregister_dependency(string table, string trigger, function fun) {
    dependencies[table][trigger] -= ({ fun });
}

//#define SYNCDB_MIGRATION_DEBUG

Thread.Mutex migration_mutex = Thread.Mutex();

object register_view(string name, object type) {
    object schema = type->schema;

    object vtable = get_version_table();
    object table;

    if (vtable) {
        mapping type_versions = schema->type_versions();
        int schema_version = schema->get_schema_version();
        Sql.Sql sql;

        mixed err = catch {
RETRY: do {
            array(object) tmp = vtable->fetch_by_table_name(name);
            object v = sizeof(tmp) && tmp[0];
            array(object) migrations = ({ });

            // protecting v
            Thread.MutexKey key;

            if (!v) {
                // does the table even exist?
                mixed err = catch {
                    Sql.Sql sql = sqlcb();

                    if (!has_value(sql->list_tables(name), name)) {
                        v = vtable->put(([
                            "table_name" : name,
                            "type_versions" : type_versions,
                            "schema_version" : schema_version,
                            "created" : Calendar.now(),
                            "migration_started" : Calendar.now(),
                        ]));

                        key = v->lock();

                        SyncDB.Migration.Base(0, schema)->create_table(name)(sql);
                        v->migration_stopped = Calendar.now();
                        v->save_unlocked();
                        destruct(key);
                        break RETRY;
                    } else {
                        object now = Calendar.now();
                        v = vtable->put(([
                            "table_name" : name,
                            "type_versions" : ([]),
                            "schema_version" : -1,
                            "created" : now,
                            "migration_started" : now,
                            "migration_stopped" : now,
                        ]));

                        key = v->lock();
                    }
                };

                // sql_state 23000
                if (objectp(err) && err->sqlstate == "23000") {
                    // insert collision on table->put above.
                    continue;
                }
                if (err) throw(err);
            } else {
                key = v->lock();

                if (v->migration_stopped->is_val_null) {
                    int since = time() - v->migration_started->unix_time();
                    // 5 minutes seems fair?
                    if (since > 5 * 60)
                        error("A Migration has been running since %d seconds ago. Probably died. Fix manually!\n", since);
#ifdef SYNCDB_MIGRATION_DEBUG
                    //werror("Observing a migration in flight on %O. wait for it.\n", name);
#endif
                    Sql.Sql sql = sqlcb();
                    if (has_value(sql->list_tables(name), name)) {
                        sql->query(sprintf("LOCK TABLES `%s` WRITE;", name));
                        sql->query("UNLOCK TABLES;");
                    }
                    sleep(0.5);
                    destruct(key);
                    continue;
                }
            }

            int current_schema_version = v->schema_version;
            mapping current_type_versions = v->type_versions;

            // nothing to do.
            if (schema_version == current_schema_version &&
                equal(type_versions, current_type_versions)) break;

            // v exists and is locked here.

            sql = sqlcb();
            sql->query(sprintf("LOCK TABLES `%s` WRITE;", name));
            
            int fail;

            v->force_update();

            // we have locked the table, v cannot change anymore
            current_schema_version = v->schema_version;
            current_type_versions = v->type_versions;

            // nothing to do.
            if (schema_version == current_schema_version &&
                equal(type_versions, current_type_versions)) {
                sql->query("UNLOCK TABLES;");
                sql = 0;
                break;
            }

            if (v->migration_stopped->is_val_null) {
                // there is another migration running, which is not done yet
                sql->query("UNLOCK TABLES;");
                sql = 0;
                continue;
            }

            // lets mark ourselves in v
            v->migration_started = Calendar.now();
            v->migration_stopped = Val.null;
            v->save_unlocked(lambda(int err) { 
                fail = err;
            });

            if (fail) { // a collision happened.
                sql->query("UNLOCK TABLES;");
                sql = 0;
                destruct(key);
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
                int t1 = gethrtime();
#ifdef SYNCDB_MIGRATION_DEBUG
                werror("Table(%O): %s\n", name, migrations[0]->describe());
#endif

                migrations[0]->migrate(sql, name);

#ifdef SYNCDB_MIGRATION_DEBUG
                werror("Table(%O): DONE in %f seconds\n", name, (gethrtime() - t1)/1E6);
#endif

                sql->query(sprintf("LOCK TABLES `%s` WRITE;", name));

                v->type_versions = migrations[0]->to->type_versions();
                v->schema_version = migrations[0]->to->get_schema_version();
                v->migration_stopped = Calendar.now();
            } else {
                // nothing at all to do. for instance for "empty" tables
                v->type_versions = schema->type_versions();
                v->schema_version = schema->get_schema_version();
                v->migration_stopped = Calendar.now();
            }

            v->save_unlocked(lambda(int err) {
                fail = err;
            });

            sql->query("UNLOCK TABLES;");

            if (fail) {
                error("Could not register the migration end.\n");
            }

            sql = 0;
            destruct(key);
            continue;
        } while (1);
        };

        if (sql) sql->query("UNLOCK TABLES;");

        if (err) throw(err);

        table = type->get_table(sqlcb, name);
    } else {
        table = type->get_previous_table(sqlcb, name, 0, ([]));
    }

    // all the above is meant to run in parallel

    object key = migration_mutex->lock();

    if (low_get_table(name, type)) error("table for %O %O already exists.\n", name, type);
    register_table(name, table);

    if (has_index(dependencies, name))
        foreach (dependencies[name]; string trigger; array(function) a)
            foreach (a;; function fun)
                table->register_trigger(trigger, fun);

    return table;
}

void unregister_view(string name, object type) {
    object table = low_get_table(name, type);
    if (table) {
        unregister_table(name, table);
        destruct(table);
    }
}

void register_table(string name, object table) {
    ::register_table(name, table);
    table->set_database(this);
}

void uregister_table(string name, object table) {
    table->set_database();
    ::unregister_table(name, table);
}

typedef function(object(SyncDB.Version),array(mapping|object):void) update_cb;

mapping(string:array(update_cb)) update_cbs = ([]);

void register_update(string table_name, update_cb cb) {
    update_cbs[table_name] += ({ cb });
}

void unregister_update(string table_name, update_cb cb) {
    if (has_index(update_cbs, table_name)) 
        update_cbs[table_name] -= ({ cb });
}

void signal_update(string|object table, object version, void|array(mapping) rows) {
    mapping t = all_tables();

    if (objectp(table)) {
        // local update, propagate to all tables globally
        string name = table->table_name();
        if (has_index(t, name)) (t[name] - ({ table }))->handle_update(version, rows);
        if (this_program::name) .signal_update(this, name, version, rows);
        if (has_index(update_cbs, name)) call_out(update_cbs[name], 0, version, rows);
    } else {
        if (has_index(t, table)) t[table]->handle_update(version, rows);
        if (has_index(update_cbs, table)) call_out(update_cbs[table], 0, version, rows);
    }
}
