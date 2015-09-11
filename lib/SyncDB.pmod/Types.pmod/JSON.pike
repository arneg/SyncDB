inherit .Simple;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.JSON();
}
#endif

string encode_sql_value(mixed val) {
    // TODO:: reencode JSON to canonical format?
    // we are using unicode mode, so in fact this needs to remain unencoded!
    return (Standards.JSON.encode(val));
}

mixed decode_sql_value(string s) {
    if (sizeof(s)) return Standards.JSON.decode(s);
    else return Val.null;
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "LONGTEXT");
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("LONGTEXT", filter_cb);
}

void generate_decode_value(object buf, string val) {
    buf->add("%H(%s)", Standards.JSON.decode, val);
}

void generate_encode_value(object buf, string val) {
    buf->add("%H(%s)", Standards.JSON.encode, val);
}

string type_name() {
    return "json";
}

int(0..1) supports_native_default() {
    return 0;
}
