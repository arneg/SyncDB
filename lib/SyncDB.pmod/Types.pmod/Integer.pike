inherit .Base;

object get_parser() {
    return Serialization.Types.Int();
}

program get_critbit() {
    return ADT.CritBit.IntTree;
}

string encode_sql_value(mixed val) {
    return sprintf("'%d'", val);
}

mixed decode_sql_value(string s) {
    return (int)s;
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Integer");
}
