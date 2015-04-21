inherit .Simple;

array(string) options;
mapping(string:int) allowed;

void create(string name, array(string) options, SyncDB.Flags.Base ... flags) {
    this_program::options = options;
    allowed = mkmapping(options, allocate(sizeof(options), 1));
    ::create(name, @flags);
}

string sql_type(Sql.Sql sql) {
    array(string) list = map(options, sql->quote);
    return ::sql_type(sql, sprintf("ENUM('%s')", list*"','"));
}

void generate_decode_value(object buf, string val) {
    buf->add(val);
}

void generate_encode_value(object buf, string val) {
    buf->add(val);
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    SyncDB.MySQL.Query sql_type = SyncDB.MySQL.Query("ENUM(" + allocate(sizeof(options), "%s") * "," + ")", @options);
    return ::column_definitions(sql_type, filter_cb);
}

string type_name() {
    return "enum";
}

int(0..1) schema_equal(mixed b) {
    return ::schema_equal(b) && equal(options, b->options);
}

int(0..1) _equal(mixed b) {
    return ::_equal(b) && equal(options, b->options);
}
