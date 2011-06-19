constant is_readable = 1;
constant is_writable = 1;

array(SyncDB.Flags.Base) flags;
int priority = 50;
string name;
string table;

mixed `->(string index) {
    if (has_prefix(index, "is_")) {
	array(int(0..1)) is = flags[index] + ({ this[index] });
	if (sizeof(is)) return max(@is);
    } else if (has_prefix(index, "f_")) {
	array t = filter(flags + ({ this }), lambda(object o) { return o["is_"+index[2..]]; });
	if (!sizeof(t)) {
	    return UNDEFINED;
	}
	return t[0];
    }
    //return this_program::`[](index);
    return this[index];
}

void create(string name, SyncDB.Flags.Base ... flags) {
    this_program::name = name;
    this_program::flags = flags;
}

mixed decode_sql_value(string s) {
    return s;
}

string encode_sql_value(mixed v) {
    return (string)v;
}

mixed decode_sql(string table, mapping row, mapping|void new) {
    string n = sql_name(table);
    if (has_index(row, n)) {
	mixed v = decode_sql_value(row[n]);
	if (new) new[name] = v;
	return v;
    }
    return UNDEFINED;
}

mapping encode_sql(string table, mapping row, mapping|void new) {
    if (!new) new = ([]);
    if (has_index(row, name))
	new[sql_name(table)] = encode_sql_value(row[name]);
    return new;
}

// TODO: this can be cached
string sql_name(string table) {
    object f = this->f_foreign;
    if (f) {
	return sprintf("%s.%s", f->table||table, f->field||name);
    }
    return sprintf("%s.%s", table, name);
}

array(string) sql_names(string table) {
    return ({ sql_name(table) });
}
