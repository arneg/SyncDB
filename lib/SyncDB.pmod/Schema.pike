mapping(string:SyncDB.Types.Base) m;
//string key;

void create(mapping(string:SyncDB.Types.Base) m) {
    this_program::m = m;
#if 0
    foreach (m; string field; SyncDB.Types.Base val) {
	if (Array.any(val->flags->is_key, `!=, 0)) {
	    key = field;
	}
    }
#endif
}

mixed `[](mixed in) {
    return m[in];
}

object parser(function|void filter) {
    mapping(string:SyncDB.Types.Base) n = ([ ]);

    foreach (m; string field, SyncDB.Types val) {
	if (!filter || filter(field, val)) {
	    n[field] = val->parser();
	}
    }

    return Serialization.Struct("_schema", n);
}

object parser_in() {
    return parser(lambda(SyncDB.Types.Base type) { return !type->is_automatic; });
}

object parser_out() {
    return parser(lambda(SyncDB.Types.Base type) { return !type->is_hidden; });
}

string json_encode() {
    return sprintf("(new SyncDB.Schema(%s))", Standards.JSON.encode(m));
}
