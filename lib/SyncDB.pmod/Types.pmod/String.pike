inherit .Base;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.String();
}
#endif

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.Tree;
}
#endif

string encode_sql_value(mixed val, function quote) {
    return sprintf("'%s'", quote(val));
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.String");
}

string sql_type() {
    return name + " LONGTEXT BINARY " + flags->sql_type() * " ";
}
