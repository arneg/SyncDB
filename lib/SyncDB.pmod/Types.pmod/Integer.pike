inherit .Base;

object parser() {
    return Serialization.Types.Int();
}

string encode_sql(int i) {
    return sprintf("'%d'", i);
}

int decode_sql(string s) {
    return (int)s;
}
