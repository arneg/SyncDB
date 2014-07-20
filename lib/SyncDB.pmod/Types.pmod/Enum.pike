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
