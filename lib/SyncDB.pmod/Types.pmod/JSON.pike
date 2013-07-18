inherit .Base;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.JSON();
}
#endif

string encode_sql_value(mixed val) {
    // TODO:: reencode JSON to canonical format?
    return string_to_utf8(Standards.JSON.encode(val));
}

mixed decode_sql_value(string s) {
    return Standards.JSON.decode_utf8(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "LONGBLOB");
}
