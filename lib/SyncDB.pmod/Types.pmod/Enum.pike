inherit .Base;

array(string) options;
mapping(string:int) allowed;

void create(string name, array(string) options, SyncDB.Flags.Base ... flags) {
    this_program::options = options;
    allowed = mkmapping(options, allocate(sizeof(options), 1));
    ::create(name, @flags);
}

string encode_sql_value(mixed val) {
    if (!allowed[val]) {
	error("bad value %O for %O\n", val, options);
    }
    return ::encode_sql_value(val);
}

string sql_type(Sql.Sql sql) {
    array(string) list = map(options, sql->quote);
    return ::sql_type(sql, sprintf("ENUM('%s')", list*"','"));
}
