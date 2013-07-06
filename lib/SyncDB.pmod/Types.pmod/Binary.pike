inherit .Base;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Binary();
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Binary");
}
