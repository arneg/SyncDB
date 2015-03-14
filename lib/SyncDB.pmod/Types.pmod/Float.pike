inherit .Simple;

#ifdef constant(Serialization)
object get_parser() {
    return Serialization.Types.Int();
}
#endif

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.FloatTree;
}
#endif

mixed decode_sql_value(string s) {
    return (float)s;
}

string encode_sql_value(mixed v) {
    return (string)v;
}

void generate_decode_value(object buf, string val) {
    buf->add("(float)%s", val);
}

void generate_encode_value(object buf, string val) {
    buf->add("(string)%s", val);
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Float");
}

#ifdef constant(Serialization)
object get_filter_parser() {
#ifdef TEST_RESOLVER
    return SyncDB.Serialization.BloomFilter(MMP.Utils.Bloom.IntHash);
#else
    return master()->resolv("SyncDB.Serialization.BloomFilter")(MMP.Utils.Bloom.IntHash);
#endif
}
#endif

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("DOUBLE", filter_cb);
}
