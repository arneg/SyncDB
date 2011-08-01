inherit ADT.CritBit.Range : range;
inherit .Vector : base;

void create(string name, object from, object to, SyncDB.Flags.Base ... flags) {
    if (object_program(to) != object_program(from))
	error("Range only work with one single type now.\n");
    base::create(name, ({ from, to }), @flags);
}

mixed `a() {
    return fields && fields[0];
}

mixed `b() {
    return fields && fields[1];
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

object get_filter_parser() {
    return Serialization.Types.RangeSet(fields[0]->get_critbit(),
					fields[0]->get_parser());
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Range", map(fields, Standards.JSON.encode));
}
