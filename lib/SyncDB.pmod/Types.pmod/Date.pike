inherit .Simple;

function(int:object)|program prog;

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.DateTree;
}
#endif

string encode_sql_value(object date) {
    if (!(objectp(date) || (!prog || Program.inherits(object_program(date), prog))))
	error("Type mismatch. Expected %O. Got %O\n", prog, date);
    return date->format_ymd();
}

object decode_sql_value(string s) {
    object date = Calendar.dwim_day(s);
    return prog ? prog(date->ux) : date;
}

#if constant(Serialization)
object get_parser() {
//Date?    return Serialization.Types.Time(prog);
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Date");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "DATE");
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("DATE", filter_cb);
}
