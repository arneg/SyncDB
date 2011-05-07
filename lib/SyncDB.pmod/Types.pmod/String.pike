inherit .Base;

object parser() {
    return Serialization.Types.Or(Serialization.Types.String(), Serialization.Types.False());
}

string encode_sql(string s) {
    return sprintf("'%s'", Sql.sql_util.quote(s));
}
