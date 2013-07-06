inherit .String;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.JSON();
}
#endif

string encode_sql_value(mixed val, function quote) {
    // TODO:: reencode JSON to canonical format?
    return ::encode_sql_value(string_to_utf8(Standards.JSON.encode(val)), quote);
}

string decode_sql_value(string s) {
    return Standards.JSON.decode_utf8(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}

string sql_type() {
    return name + " LONGBLOB " + flags->sql_types() * " ";
}
