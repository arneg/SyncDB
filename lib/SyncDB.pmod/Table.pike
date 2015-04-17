string dbname; 
SyncDB.Schema schema;
SyncDB.Table db;
SyncDB.Version version;

void create(mixed dbname, mixed schema, void|mixed db) {
    this_program::dbname = dbname;
    this_program::db = db;
    this_program::schema = schema;
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
