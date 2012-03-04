inherit .Base;

object get_parser() {
    return Serialization.Types.Image();
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Image");
}
