string dbname; 
SyncDB.Schema schema;
SyncDB.Table db;

void create(mixed dbname, mixed schema, void|mixed db) {
    this_program::dbname = dbname;
    this_program::db = db;
    this_program::schema = schema;
}
