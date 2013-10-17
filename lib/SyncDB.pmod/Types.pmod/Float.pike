inherit .Base;

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

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "DOUBLE");
}
