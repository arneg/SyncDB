string dbname; 
SyncDB.Schema schema;
SyncDB.Table db;
int generation = 0;

int table_generation() {
    return generation;
}

void create(mixed dbname, mixed schema, void|mixed db) {
    this_program::dbname = dbname;
    this_program::db = db;
    this_program::schema = schema;
    register_trigger("after_change", after_change);
}

void after_change(object table) {
    generation++;
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

int(0..1) has_triggers(string name) {
    array triggers = get_triggers(name);
    return triggers && sizeof(triggers);
}

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
