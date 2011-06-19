inherit .Base;

object parser() {
    return Serialization.Types.Or(Serialization.Types.String(), Serialization.Types.False());
}

string encode_sql_value(mixed val) {
    return sprintf("'%s'", Sql.sql_util.quote(val));
}

string encode_json() {
    return ::encode_json("SyncDB.Types.String");
}
