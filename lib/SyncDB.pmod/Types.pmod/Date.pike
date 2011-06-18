inherit .Base;

program prog;

void create(string name, program prog, SyncDB.Flags.Base ... flags) {
    this_program::prog = prog;
    ::create(name, @flags);
}

string encode_sql(object date) {
    // TODO:: mysql-FROM_UNIXTIME is not an option, use ints?
    return sprintf("FROM_UNIXTIME(%d)", date->ux + date->utc_offset());
}

object decode_sql(string s) {
    return prog(Calendar.dwim_time(s + " UTC")->ux);
}

object parser() {
    return Serialization.Types.Time(SyncDB.Date);
}
