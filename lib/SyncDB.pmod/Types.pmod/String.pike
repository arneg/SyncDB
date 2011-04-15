inherit .Base;

object parser() {
    return Serialization.Types.String();
}

string encode_sql(string s) {
    return sprintf("'%s'", Sql.sql_util.quote(s));
}
