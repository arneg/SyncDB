inherit .Base;

program prog;

void create(program prog, SyncDB.Flags.Base ... flags) {
    this_program::prog = prog;
    ::create(@flags);
}

string encode_sql(object date) {
    return sprintf("FROM_UNIXTIME(%d)", date);
}

object decode_sql(string s) {
    return prog(Calendar.dwim_time(s + " UTC")->ux);
}
