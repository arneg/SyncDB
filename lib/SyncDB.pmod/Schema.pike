mapping(string:SyncDB.Types.Base) m;
string key;
array(string) index = ({ });
string automatic;

void create(mapping(string:SyncDB.Types.Base) m) {
    this_program::m = m;
#if 1
    foreach (m; string field; SyncDB.Types.Base val) {
	if (val->is_index) index += ({ field });
	if (val->is_key) {
	    if (key) error("...");
	    key = field;
	}
	if (val->is_automatic) {
	    if (automatic) error("...");
	    automatic = field;
	}

    }
#endif
}

mixed `[](mixed in) {
    return m[in];
}

object parser(function|void filter) {
    mapping(string:SyncDB.Types.Base) n = ([ ]);

    foreach (m; string field; SyncDB.Types.Base val) {
	if (!filter || filter(field, val)) {
	    n[field] = val->parser();
	}
    }

    return Serialization.Types.Struct("_schema", n);
}

object parser_in() {
    return parser(lambda(string field, SyncDB.Types.Base type) {
	return !type->is_automatic; 
    });
}

object parser_out() {
    return parser(lambda(string field, SyncDB.Types.Base type) {
	  return !type->is_hidden; 
    });
}

string json_encode() {
    return sprintf("(new SyncDB.Schema(%s))", Standards.JSON.encode(m));
}
