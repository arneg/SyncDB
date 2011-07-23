inherit .Base;

object get_parser() {
    return Serialization.Types.String();
}

program get_critbit() {
    return ADT.CritBit.Tree;
}

string encode_sql_value(mixed val) {
    return sprintf("'%s'", Sql.sql_util.quote(val));
}

string encode_json() {
    return ::encode_json("SyncDB.Types.String");
}
