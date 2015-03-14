inherit .Simple;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Binary();
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Binary");
}

void generate_decode_value(object buf, string val) {
    buf->add(val);
}

void generate_encode_value(object buf, string val) {
    buf->add(val);
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("BLOB", filter_cb);
}
