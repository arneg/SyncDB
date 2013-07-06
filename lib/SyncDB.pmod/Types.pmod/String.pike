inherit .Base;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.String();
}
#endif

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.Tree;
}
#endif

string encode_sql_value(mixed val, function quote) {
    return sprintf("'%s'", quote(string_to_utf8(val)));
}

string decode_sql_value(string s) {
    return utf8_to_string(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.String");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "LONGTEXT BINARY");
}
