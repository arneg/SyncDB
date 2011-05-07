inherit .Base;

object parser() {
    return Serialization.Types.Or(Serialization.Types.Int(), Serialization.Types.False());
}

string encode_sql(int i) {
    return sprintf("'%d'", i);
}

int decode_sql(string s) {
    return (int)s;
}
