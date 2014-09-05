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
