inherit .Base;

object parser() {
    return Serialization.Or(Serialization.Types.Int(), Serialization.Types.False());
}

string encode_sql(int i) {
    return sprintf("'%d'", i);
}

int decode_sql(string s) {
    return (int)s;
}
