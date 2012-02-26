inherit .String;

object get_parser() {
    return Serialization.Types.JSON();
}

program get_critbit() {
    return ADT.CritBit.Tree;
}

string encode_sql_value(mixed val) {
    // TODO:: reencode JSON to canonical format?
    return sprintf("'%s'", Sql.sql_util.quote(Standards.JSON.encode(val)));
}

string decode_sql_value(string s) {
    return Standards.JSON.decode(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}
