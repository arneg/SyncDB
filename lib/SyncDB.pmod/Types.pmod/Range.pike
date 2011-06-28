inherit .Vector;

void create(string name, object from, object to, SyncDB.Flags.Base ... flags) {
    if (object_program(to) != object_program(from))
	error("Range only work with one single type now.\n");
    ::create(name, ({ from, to }), @flags);
}

mapping encode_sql(string table, mapping row, void|mapping new) {
    if (!new) new = ([]);
    if (has_index(row, name)) {
	SyncDB.Interval i = row[name];
	mapping t = row + ([ name : ({ i->start, i->stop }) ]);
	return ::encode_sql(table, t, new);
    }
    return new;
}

mixed decode_sql(string table, mapping row, void|mapping new) {
    mixed v = ::decode_sql(table, row);

    if (arrayp(v)) {
	v = SyncDB.Interval(@v);
	if (new) new[name] = v;	
    }
    return v;
}

object get_parser() {
#ifdef TEST_RESOLVER
    return SyncDB.Serialization.Range(fields[0]->parser());
#else
    return master()->resolv("SyncDB.Serialization.Range")(fields[0]->parser());
#endif
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Range", map(fields, Standards.JSON.encode));
}
