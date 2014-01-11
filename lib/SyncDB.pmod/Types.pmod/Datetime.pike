inherit .Base;

function(int:object)|program prog;

constant _types = ::_types + ([
    "prog" : -1
]);

void create(string name, SyncDB.Flags.Base ... flags) {
    ::create(name, @flags);
}

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.DateTree;
}
#endif

string encode_sql_value(object datetime) {
    // TODO:: mysql-FROM_UNIXTIME is not an option, use ints?
    if (!(objectp(datetime) || (!prog || Program.inherits(object_program(datetime), prog))))
	error("Type mismatch. Expected %O. Got %O\n", prog, datetime);
    return datetime->set_timezone("UTC")->format_time();
}

object decode_sql_value(string s) {
    object datetime = Calendar.dwim_time(s + " UTC");
    return prog ? prog(datetime->ux) : datetime;
}

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Time(prog);
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Datetime");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "DATETIME");
}