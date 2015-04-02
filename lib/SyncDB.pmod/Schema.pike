//! Array of all fields.
array(SyncDB.Types.Base) fields;

//! Mapping containing @[fields], indexed by their names.
mapping(string:SyncDB.Types.Base) m;

string automatic;

//! If the schema contains a unique key field, this one will be it.
object id;

//! If the schema contains a version field, this one will be it.
object version;

//! Name of the @[id].
string `key() {
    return id?->name;
}

array(object) migrations = ({ });
private array(object) index_list = ({ });

int get_schema_version() {
    return sizeof(migrations);
}

array(object) get_indices() {
    return index_list;
}

//! Row of default values associated to this schema.
mapping default_row = ([]);

string _sprintf(int t) {
    return sprintf("%O(%O)", this_program, fields);
}

void create(object ... m) {
    this_program::m = ([ ]);
    map(m, add_type);
    add_type(version = SyncDB.Types.Version("version", tables(),
                                            SyncDB.Flags.Automatic(),
					    SyncDB.Flags.Unique(),
					    SyncDB.Flags.Index()));
}

mixed get_unique_identifier(mapping row) {
    return (string)row->version;
}

this_program `+(this_program o) {
    array(object) la = fields - ({ m->version });
    array(object) lb = o->fields - ({ o["version"] });
    // reversing the order here doesnt change anything, but makes the table structure a little more natural.
    return this_program(@(lb + la));
}

mixed `[](mixed in) {
    return m[in];
}

array(SyncDB.Types.Base) index_fields() {
    return filter(fields, fields->is->index);
}

array(SyncDB.Types.Base) unique_fields() {
    return filter(fields, fields->is->unique);
}

void add_type(object type) {
    if (has_index(m, type->name)) fields = filter(fields, `!=, m[type->name]);
    if (Program.inherits(object_program(type), SyncDB.MySQL.Filter.Base)) {
        error("Restriction support has been removed.\n");
    }
    if (type->is->index) {
        index_list += ({ .Indices.Btree(type->name, type) });
    }
    if (type->is->key) {
        if (id) error("Defined two different keys in one schema.\n");
        id = type;
    }
    if (type->is->automatic) {
        if (automatic) error("Defined two different auto-increment values in one schema\n");
        automatic = type->name;
    }
    fields += ({ type });
    m[type->name] = type;
    fields->get_default(default_row);
}

#if constant(Serialization)
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
	return type->is->writable && !type->is->automatic || field == "version" || type->is->key;
    });
}

object parser_out() {
    return parser(lambda(string field, SyncDB.Types.Base type) {
	  return type->is->readable && !type->is->hidden; 
    });
}
#endif

string encode_json() {
    return sprintf("(new SyncDB.Schema(%s))", filter(map(fields, Standards.JSON.encode), sizeof)*(",\n"+" "*4));
}

Iterator _get_iterator() {
    return get_iterator(fields);
}

array(string) tables() {
    mapping t = ([ ]);
    foreach (m; string name; object type) {
	if (!type->is->link) continue;
	t += type->f_link->tables;
    }
    return sort(indices(t));
}

private mapping(string:object) coders = ([]);

object generate_coder(string table) {
    object buf = SyncDB.CodeGen();

    buf->add("mapping decode_sql(mapping row) {\n"
             "mapping new = mkmapping(%c, allocate(%d, Val.null));\n"
             "mixed v;\n", fields->name, sizeof(fields));
    foreach (fields;; object f) {
        if (f->generate_decode) f->generate_decode(buf, table);
        else buf->add("%H(%c, row, new);\n", f->decode_sql, table);
    }
    buf->add(" return new;\n }");

    buf->add("mapping encode_sql(mapping row) {\n"
             "mapping new = ([]);\n"
             "mixed v;\n", fields->name, sizeof(fields));
    foreach (fields;; object f) {
        if (f->generate_encode) f->generate_encode(buf, table);
        else buf->add("%H(%c, row, new);", f->encode_sql, table);
    }
    buf->add(" return new;\n }");

    program p = buf->compile(sprintf("Coder<%s>", table));
    return p();
}

mapping decode_sql(string table, mapping row) {
    object coder;

    if (!has_index(coders, table)) {
        coders[table] = coder = generate_coder(table);
    } else coder = coders[table];

    mapping new = coder->decode_sql(row);

    foreach (default_row; string s; mixed v) {
        if (!has_index(new, s) || objectp(new[s]) && new[s]->is_val_null)
            new[s] = v;
    }
    return new;
}

mapping encode_sql(string table, mapping row) {
    object coder;

    if (!has_index(coders, table)) {
        coders[table] = coder = generate_coder(table);
    } else coder = coders[table];

    return coder->encode_sql(row);
}
