constant is_readable = 1;
constant is_writable = 1;

array(SyncDB.Flags.Base) flags;
int priority = 50;

mixed `->(string index) {
    if (has_prefix(index, "is_")) {
	array(int(0..1)) is = flags[index] + ({ this[index] });
	if (sizeof(is)) return max(@is);
    } else if (has_prefix(index, "f_")) {
	array t = filter(flags, lambda(object o) { return o["is_"+index[2..]]; });
	if (!sizeof(t)) {
	    return UNDEFINED;
	}
	return t[0];
    }
    //return this_program::`[](index);
    return this[index];
}

void create(SyncDB.Flags.Base ... flags) {
    this_program::flags = flags;
}

string decode_sql(string s) {
    return s;
}
