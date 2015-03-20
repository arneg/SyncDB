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
    if (schema && schema[name]) return schema[name];
    return call_function(::`->, name, this);
}

// update triggerd by this table
void signal_update(SyncDB.Version nversion, void|array(mapping) rows) {
    // TODO: we might lose updates here.
    version = nversion;
    //handle_update(nversion, rows);

    if (database) {
        database->signal_update(this, nversion, rows);
    }
}

object database;

void set_database(void|object o) {
    database = o;
}

// update triggered from somewhere else
void handle_update(SyncDB.Version nversion, void|array(mapping) rows) {
    version = nversion;
}

void destroy() {
    if (database) {
        database->unregister_table(this);
        database = 0;
    }
}

SyncDB.Version table_version() {
    return version;
}

object remote_table(string name, void|program prog) {
    if (!database) error("Cannot access remote tables without database.\n");
    return database->get_table(name, prog);
}
