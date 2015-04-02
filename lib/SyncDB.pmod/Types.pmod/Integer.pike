inherit .Simple;

#ifdef constant(Serialization)
object get_parser() {
    return Serialization.Types.Int();
}
#endif

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.IntTree;
}
#endif

string encode_sql_value(mixed val) {
    return (string)val;
}

mixed decode_sql_value(string s) {
    return (int)s;
}

void generate_encode_value(object buf, string val) {
    buf->add("((string)v)");
}

void generate_decode_value(object buf, string val) {
    buf->add("((int)v)");
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Integer");
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

string sql_type(Sql.Sql s) {
    return ::sql_type(s, "BIGINT");
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("BIGINT", filter_cb);
}

string type_name() {
    return "integer";
}
