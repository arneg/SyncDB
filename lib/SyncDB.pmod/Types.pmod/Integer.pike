inherit .Base;

object parser() {
    return Serialization.Types.Or(Serialization.Types.Int(), Serialization.Types.False());
}

string encode_sql_value(mixed val) {
    return sprintf("'%d'", val);
}

mixed decode_sql_value(string s) {
    return (int)s;
}
