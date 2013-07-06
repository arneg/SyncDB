inherit .Base;

function(int:object)|program prog;

constant _types = ::_types + ([
    "prog" : -1
]);

void create(string name, void|function(int:object)|program prog, SyncDB.Flags.Base ... flags) {
    this_program::prog = prog;
    ::create(name, @flags);
}

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.DateTree;
}
#endif

string encode_sql_value(object date) {
    // TODO:: mysql-FROM_UNIXTIME is not an option, use ints?
    if (!(objectp(date) && (!prog || Program.inherits(object_program(date), prog))))
	error("Type mismatch. Expected %O. Got %O\n", prog, date);
    return sprintf("FROM_UNIXTIME(%d)", date->ux + date->utc_offset());
}

object decode_sql_value(string s) {
    object date = Calendar.dwim_time(s + " UTC");
    return prog ? prog(date->ux) : date;
}

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Time(prog);
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Date");
}

string sql_type() {
    return name + " DATETIME " + flags->sql_type() * " ";
}
