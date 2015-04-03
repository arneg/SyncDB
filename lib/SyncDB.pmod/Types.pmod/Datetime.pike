inherit .Simple;

function(int:object)|program prog;

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.DateTree;
}
#endif

string encode_sql_value(object datetime) {
#if 1
    if (!(objectp(datetime) || (!prog || Program.inherits(object_program(datetime), prog))))
	error("Type mismatch. Expected %O. Got %O\n", prog, datetime);
    return datetime->set_timezone("UTC")->format_time();
#else
    // TODO: experimental, not sure if this loop always terminates. what happens
    // happens around changes from or to daylight saving time?
    object t = System.TM();
    int ux = datetime->ux;
    int d;
    t->gmtime(ux);
    // this date is to be interpreted in utc
    while ((d = t->unix_time() - t->gmtoff) != ux) {
        t->gmtime(ux + t->gmtoff);
    }
    return t->strftime("%Y-%m-%d %H:%M:%S");
#endif
}

object decode_sql_value(string s) {
#if constant(System.TM)
    object tm = System.TM();
    if (!tm->strptime("%Y-%m-%d %H:%M:%S", s)) {
        if (s != "0000-00-00 00:00:00")
            werror("Parsing %O as NULL\n", s);
        return Val.null;
    }
    return Calendar.Second("unix", tm->unix_time() + tm->gmtoff);
#else
    object datetime = Calendar.parse("%Y-%M-%D %h:%m:%s %z", s + " UTC");
    return prog ? prog(datetime->ux) : datetime;
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

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return ::column_definitions("DATETIME", filter_cb);
}

string type_name() {
    return "datetime";
}
