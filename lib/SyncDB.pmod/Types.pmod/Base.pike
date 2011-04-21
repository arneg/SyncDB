array(SyncDB.Flags.Base) flags;
int priority = 50;

mixed `->(string index) {
    if (has_prefix(index, "is_")) return max(@flags[index]);
    //return this_program::`[](index);
    return this[index];
}

void create(SyncDB.Flags.Base ... flags) {
    this_program::flags = flags;
}

string decode_sql(string s) {
    return s;
}
