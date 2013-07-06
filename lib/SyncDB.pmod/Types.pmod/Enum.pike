inherit .Base;

array(string) options;
mapping(string:int) allowed;

void create(string name, array(string) options, SyncDB.Flags.Base ... flags) {
    this_program::options = options;
    allowed = mkmapping(options, allocate(sizeof(options), 1));
    ::create(name, @flags);
}

string encode_sql_value(mixed val, function quote) {
    if (!allowed[val]) {
	error("bad value %O for %O\n", val, options);
    }
    return ::encode_sql_value(val, quote);
}

string sql_type(Sql.Sql sql) {
    function(mixed:string) enc = sql_encode_cb(sql);
    array(string) list = map(options, enc);
    return ::sql_type(sql, sprintf("ENUM(%s)", list*","));
}
