mapping(string:SyncDB.Types.Base) m;
array(SyncDB.Types.Base) fields;
string key;
SyncDB.Types.Base id;
array(string) index = ({ });
string automatic;

void create(object ... m) {
    fields = m;
    this_program::m = mkmapping(m->name, m);
#if 1
    foreach (fields;; SyncDB.Types.Base type) {
	if (type->name == "version") {
	    error("field called 'version' is reserved.");
	}
	if (type->is_index) index += ({ type->name });
	if (type->is_key) {
	    if (key) error("...");
	    key = type->name;
	    id = type;
	}
	if (type->is_automatic) {
	    if (automatic) error("...");
	    automatic = type->name;
	}
    }
#endif
    add_type(SyncDB.Types.Version("version", tables()));
}

mixed `[](mixed in) {
    return m[in];
}

void add_type(object type) {
    if (has_index(m, type->name)) fields = filter(fields, `!=, m[type->name]);
    fields += ({ type });
    m[type->name] = type;
}

object parser(function|void filter) {
    mapping(string:SyncDB.Types.Base) n = ([ ]);

    foreach (m; string field; SyncDB.Types.Base type) {
	if (!filter || filter(field, type)) {
	    n[field] = type->parser();
	}
    }

    return Serialization.Types.Struct("_row", n)
	|  Serialization.Types.Struct("_delete", ([
		"version" : n["version"],
		key : n[key]
	      ]), SyncDB.DeletedRow);
}

object parser_in() {
    return parser(lambda(string field, SyncDB.Types.Base type) {
	return type->is_writable && !type->is_automatic || field == "version" || type->is_key;
    });
}

object parser_out() {
    return parser(lambda(string field, SyncDB.Types.Base type) {
	  return type->is_readable && !type->is_hidden; 
    });
}

string encode_json() {
    return sprintf("(new SyncDB.Schema(%s))", filter(map(fields, Standards.JSON.encode), sizeof)*(",\n"+" "*4));
}

Iterator _get_iterator() {
    return get_iterator(fields);
}

array(string) tables() {
    mapping t = ([ ]);
    foreach (m; string name; object type) {
	if (!type->is_link) continue;
	t += type->f_link->tables;
    }
    return sort(indices(t));
}
