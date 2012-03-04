inherit .Base;

object get_parser() {
    return Serialization.Types.Int();
}

program get_critbit() {
    return ADT.CritBit.IntTree;
}

string encode_sql_value(mixed val, function quote) {
    return sprintf("'%d'", val);
}

mixed decode_sql_value(string s) {
    return (int)s;
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Integer");
}

object get_filter_parser() {
#ifdef TEST_RESOLVER
    return SyncDB.Serialization.BloomFilter(MMP.Utils.Bloom.IntHash);
#else
    return master()->resolv("SyncDB.Serialization.BloomFilter")(MMP.Utils.Bloom.IntHash);
#endif
}
