inherit .Base;

function(int:object)|program prog;

string encode_sql_value(object time) {
    // TODO:: mysql-FROM_UNIXTIME is not an option, use ints?
    if (!(objectp(time) || (!prog || Program.inherits(object_program(time), prog))))
	error("Type mismatch. Expected %O. Got %O\n", prog, time);
    return time->format_time();
}

object decode_sql_value(string s) {
    object time = Calendar.parse("%Y-%M-%D %h:%m:%s %z", Calendar.now()->format_ymd() + " " + s + " UTC");
    return prog ? prog(time->ux) : time;
}

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Time(prog);
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Time");
}

string sql_type(Sql.Sql sql) {
    return ::sql_type(sql, "TIME");
}
