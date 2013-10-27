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
    handle_update(nversion, rows);

    if (update_manager) {
        update_manager->signal_update(this, nversion, rows);
    }
}

object update_manager;

// update triggered from somewhere else
void handle_update(SyncDB.Version nversion, void|array(mapping) rows) {
    version = nversion;
}

void destroy() {
    if (update_manager) update_manager->unregister_table(this);
}
