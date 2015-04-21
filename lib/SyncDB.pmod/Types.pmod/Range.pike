inherit .Vector;

void create(string name, object from, object to, SyncDB.Flags.Base ... flags) {
    if (object_program(to) != object_program(from))
	error("Range only work with one single type now.\n");
    ::create(name, ({ from, to }), @flags);
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
	ADT.Interval i = row[name];
	mapping t = row + ([ name : ({ i->start, i->stop }) ]);
	return ::encode_sql(table, t, new);
    }
    return new;
}

mixed decode_sql(string table, mapping row, void|mapping new) {
    mixed v = ::decode_sql(table, row);

    if (arrayp(v)) {
	v = ADT.Interval(@v);
	if (new) new[name] = v;	
    }
    return v;
}

#if constant(Serialization)
object get_parser() {
#ifdef TEST_RESOLVER
    return SyncDB.Serialization.Range(fields[0]->parser());
#else
    return master()->resolv("SyncDB.Serialization.Range")(fields[0]->parser());
#endif
}

object get_filter_parser() {
    return
#ifdef TEST_RESOLVER
	SyncDB.Serialization.OverlapsFilter
#else
	master()->resolv("SyncDB.Serialization.OverlapsFilter")
#endif
	    (fields[0]->get_critbit(), fields[0]->get_parser());
}
#endif

string encode_json() {
    return ::encode_json("SyncDB.Types.Range", map(fields, Standards.JSON.encode));
}

string type_name() {
    return "range";
}
