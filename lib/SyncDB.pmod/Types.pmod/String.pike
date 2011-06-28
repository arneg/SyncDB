inherit .Base;

object get_parser() {
    return Serialization.Types.String();
}

string encode_sql_value(mixed val) {
    return sprintf("'%s'", Sql.sql_util.quote(val));
}

string encode_json() {
    return ::encode_json("SyncDB.Types.String");
}
