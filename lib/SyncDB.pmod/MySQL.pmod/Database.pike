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

    return has_value(sql->list_tables(version_table_name), version_table_name);
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

object register_view(string name, object type) {
    object schema = type->schema;

    object vtable = get_version_table();
    object table;

    if (vtable) {
        mapping type_versions = schema->type_versions();
        int schema_version = schema->get_schema_version();
        Sql.Sql sql;
        array(object) tmp;

RETRY: do {
            tmp = vtable->fetch_by_table_name(name);
            object v = sizeof(tmp) && tmp[0];

            if (!v) {
                // does the table even exist?
                Sql.Sql sql = sqlcb();
                mixed err = catch {
                    if (!has_value(sql->list_tables(name), name)) {
                        werror("creating table %s\n", name);
                        v = vtable->put(([
                            "table_name" : name,
                            "type_versions" : type_versions,
                            "schema_version" : schema_version,
                            "created" : Calendar.now(),
                            "migration_started" : Calendar.now(),
                        ]));
                        Thread.MutexKey key = v->lock();
                        SyncDB.Migration.Base(0, schema)->create_table(name)(sql);
                        v->migration_stopped = Calendar.now();
                        v->save_unlocked();
                        destruct(key);
                        break RETRY;
                    }
                };

                // sql_state 23000
                if (err && err->sqlstate == "23000") {
                    // insert collision on table->put above.
                    continue;
                }
                if (err) throw(err);
            }

            if (!v || schema_version != v->schema_version || !equal(type_versions, v->type_versions)) {
                if (v && schema_version < v->schema_version)
                    error("Requested Schema version older than database.\n");
                if (!sql) {
                    sql = sqlcb();
                    sql->query(sprintf("LOCK TABLES `%s` WRITE;", name));
                    tmp = vtable->fetch_by_table_name(name);
                    continue;
                }

                // we have caught some other migration happening
                if (v && v->migration_stopped->is_val_null) {
                    werror("Some other thread/process is currently migrating table %O. Unlock and retry.\n", name);
                    sql->query("UNLOCK TABLES;");
                    sql = 0;
                    int since = time() - v->migration_started->unix_time();
                    // 5 minutes seems fair?
                    if (since > 5 * 60)
                        error("A Migration has been running since %d seconds ago. Probably died progress. Fix manually!\n");
                    sleep(0.25);
                    continue;
                }

                array(object) migrations; 

                if (!v) {
                    object initial_schema = schema->get_previous_schema(0, ([]));
                    migrations = ({
                        SyncDB.Migration.Simple(initial_schema, initial_schema)
                    });
                    mixed err = catch {
                        v = vtable->put(([
                            "table_name" : name,
                            "type_versions" : ([]),
                            "schema_version" : 0,
                            "created" : Calendar.now(),
                        ]));
                    };
                    if (err) {
                        if (err->sqlstate == "23000") continue;

                        throw(err);
                    }
                } else migrations = ({ });

                migrations += schema->get_migrations(v->schema_version, v->type_versions);

                // locking this late is fine, as threads will anyway have to lock the table which needs to
                // be migrated.
                Thread.MutexKey key = v->lock();

                v->migration_started = Calendar.now();
                v->migration_stopped = Val.null;
                v->save_unlocked();

                int t1 = gethrtime();
                werror("Migrating table %s with %O\n", name, migrations[0]);

                migrations[0]->migrate(sql, name);

                werror("Migrated table %s in %f seconds\n", name, (gethrtime() - t1)/1E6);

                v->type_versions = migrations[0]->to->type_versions();
                v->schema_version = migrations[0]->to->get_schema_version();
                v->migration_stopped = Calendar.now();
                v->save_unlocked();

                sql = 0;
                destruct(key);
            } else if (v->migration_stopped->is_val_null) {
                werror("Observing a migration in flight on %O\n", name);
                sleep(0.25);
                continue;
            }
            if (sql) sql->query("UNLOCK TABLES;");
            break;
        } while (1);

        table = type->get_table(sqlcb, name);
    } else {
        table = type->get_previous_table(sqlcb, name, 0, ([]));

        if (low_get_table(name, type)) error("table for %O %O already exists.\n", name, type);
    }

    register_table(name, table);

    if (has_index(dependencies, name))
        foreach (dependencies[name]; string trigger; array(function) a)
            foreach (a;; function fun)
                table->register_trigger(trigger, fun);

    return table;
}

void unregister_view(string name, object type) {
    object table = low_get_table(name, type);
    unregister_table(name, table);
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
