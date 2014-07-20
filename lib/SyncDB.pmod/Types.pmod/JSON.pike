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
    return Standards.JSON.decode(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "LONGTEXT");
}

void generate_decode_value(object buf, string val) {
    buf->add("%H(%s)", Standards.JSON.decode, val);
}

void generate_encode_value(object buf, string val) {
    buf->add("%H(%s)", Standards.JSON.encode, val);
}
