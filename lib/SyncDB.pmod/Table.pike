string dbname; 
SyncDB.Schema schema;
SyncDB.Table db;
SyncDB.Version version;

void create(mixed dbname, mixed schema, void|mixed db) {
    this_program::dbname = dbname;
    this_program::db = db;
    this_program::schema = schema;
    register_trigger("after_update", after_update);
    register_trigger("after_delete", after_delete);
    register_trigger("after_insert", after_insert);
}

void after_insert(object table, mapping row) {
    if (table == this) return;
    version = row->version;
}

void after_update(object table, mapping row, mapping changes) {
    if (table == this) return;
    version = row->version;
}

void after_delete(object table, mapping keys) {
    if (table == this) return;
    if (keys->version) {
        version = -keys->version;
    } else if (!version->is_deleted()) {
        // FIXME: the table version remains here...
        version = -version; 
    }
}

mixed `->(string name) {
    if (object_variablep(this, name)) return call_function(::`->, name, this);

    mixed v = call_function(::`->, name, this);

    if (!undefinedp(v)) return v;

    if (schema && schema[name]) return schema[name];

    return UNDEFINED;
}

final object database;

mapping(string:array) triggers = ([]);

void register_trigger(string event, function f) {
    if (!functionp(f)) error("Bad argument.\n");
    if (!triggers[event]) triggers[event] = ({ f });
    else triggers[event] += ({ f });
    if (database) database->register_trigger(dbname, event, f);
}

void unregister_trigger(string event, function f) {
    triggers[event] -= ({ f });
    if (database) database->unregister_trigger(dbname, event, f);
}

void set_database(void|object o) {
    if (database) {
        foreach (triggers; string event; array a) {
            foreach (a;; function f) {
                database->unregister_trigger(dbname, event, f);
            }
        }
    }

    database = o;

    if (database) {
        foreach (triggers; string event; array a) {
            foreach (a;; function f) {
                database->register_trigger(dbname, event, f);
            }
        }
    }
}

// different types of triggers
// before_insert ( new row )
// after_insert ( new row )
// before_update ( old row, changes )
// after_update ( new row, changes )
// before_delete ( keys )
// after_delete ( keys )

array get_triggers(string name) {
    if (database) return database->get_triggers(dbname, name);
    return triggers[name] || ({ });
}

void trigger(string event, mixed ... args) {
    array triggers = get_triggers(event);

    if (triggers && sizeof(triggers)) {
        if (has_prefix(event, "before_")) {
            triggers(this, @args);
        } else {
            foreach (triggers;; mixed f) {
                mixed err = catch(f(this, @args));
                if (err) master()->handle_error(err);
            }
        }
    }
}

SyncDB.Version table_version() {
    return version;
}

object remote_table(string name, void|program prog) {
    if (!database) error("Cannot access remote tables without database.\n");
    return database->get_table(name, prog);
}

void destroy() {
    if (database) {
        werror("%O destructed while having a database set.\n", this);
        master()->handle_error(catch(error("bar")));
        set_database();
    }
}
