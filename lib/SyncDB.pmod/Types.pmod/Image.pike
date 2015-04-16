inherit .Binary;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Image();
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Image");
}

string type_name() {
    return "image";
}
