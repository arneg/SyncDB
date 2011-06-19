inherit .Base;

program prog;

void create(string name, program prog, SyncDB.Flags.Base ... flags) {
    this_program::prog = prog;
    ::create(name, @flags);
}

string encode_sql_value(object date) {
    // TODO:: mysql-FROM_UNIXTIME is not an option, use ints?
    if (!Program.inherits(object_program(date), prog))
	error("Type mismatch. Expected %O. Got %O\n", prog, date);
    return sprintf("FROM_UNIXTIME(%d)", date->ux + date->utc_offset());
}

object decode_sql_value(string s) {
    return prog(Calendar.dwim_time(s + " UTC")->ux);
}

object parser() {
    return Serialization.Types.Time(prog);
}
