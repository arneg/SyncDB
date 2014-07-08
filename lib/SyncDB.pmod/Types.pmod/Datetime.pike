inherit .Base;

function(int:object)|program prog;

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
    object datetime = Calendar.parse("%Y-%M-%D %h:%m:%s %z", s + " UTC");
    return prog ? prog(datetime->ux) : datetime;
#if constant(System.TM)
    // TODO: this does not properly account for timezone UTC
    object tm = System.TM();
    if (!tm->strptime("%Y-%m-%d %H:%M:%S", s)) {
        error("Could not Parse %O\n", s);
    }
    return Calendar.Second("unix", tm->unix_time() - System.TM(1970, 0, 1, 0, 0, 0)->unix_time());
#else
    return Calendar.Second("unix", (int)s);
#endif
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
