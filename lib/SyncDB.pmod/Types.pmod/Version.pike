inherit .Vector;

void create(string name, array(string) tables, SyncDB.Flags.Base ... flags) {
    array a = allocate(sizeof(tables)+1);
    a[0] = SyncDB.Types.Integer("_version_0", SyncDB.Flags.Foreign(0, "version"));
    for (int i = 1; i < sizeof(a); i++) {
	a[i] = SyncDB.Types.Integer(sprintf("_version_%d", i),
				SyncDB.Flags.Foreign(tables[i-1], "version"));
    }
    flags += ({ SyncDB.Flags.ReadOnly() });
    ::create(name, a, @flags);
}

object get_parser() {
    object o = ::get_parser();
    o->type = "_version";
    o->constructor = lambda (int ... a) { return SyncDB.Version(a); };
    return o;
}

string encode_json() {
    return ::encode_json("SyncDB.Types.Version", ({ (string)sizeof(fields) }));
}

mapping encode_sql(string table, mapping row, void|mapping new) {
    if (has_index(row, name))
	row += ([ name : row[name]->a ]);

    return ::encode_sql(table, row, new);
}

SyncDB.Version|mapping decode_sql(string table, mapping row, void|mapping new) {
    if (new) {
	::decode_sql(table, row, new);
	new[name] = SyncDB.Version(new[name]);
	return new;
    } else return SyncDB.Version(::decode_sql(table, row));
}
