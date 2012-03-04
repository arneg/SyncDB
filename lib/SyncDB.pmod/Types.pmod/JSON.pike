inherit .String;

object get_parser() {
    return Serialization.Types.JSON();
}

program get_critbit() {
    return ADT.CritBit.Tree;
}

string encode_sql_value(mixed val, function quote) {
    // TODO:: reencode JSON to canonical format?
    return ::encode_sql_value(Standards.JSON.encode(val), quote);
}

string decode_sql_value(string s) {
    return Standards.JSON.decode(s);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.JSON");
}
