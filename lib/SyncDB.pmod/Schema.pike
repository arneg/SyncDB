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


mapping(string:int) type_versions() {
    mapping(string:int) m = ([]);
    fields->type_versions(m);
    return m;
}

this_program get_previous_schema(int schema_version, mapping(string:int)|void versions) {
    if (schema_version != sizeof(migrations)) {
        if (schema_version > sizeof(migrations))
            error("Schema is older than requested version: %d vs %d.\n", schema_version, sizeof(migrations));
        return migrations[schema_version]->from->get_previous_schema(schema_version, versions);
    }

    if (!versions) return this;

    mapping my_versions = type_versions();
    array fields = this_program::fields;

    foreach (my_versions; string name; int version) {
        int requested_version = (int)versions[name];
        if (version == requested_version) continue;
        if (version < requested_version) error("Type version inversion. SyncDB version is too old.\n");

        fields = fields->get_previous_type(name, requested_version);
    }

    this_program prev = this_program(@fields);
    prev->migrations = migrations;
    return prev;
}

object get_migration(string type_name, object from) {
    array(object) ret = fields->get_migration(type_name, from, this);
    ret = filter(ret, ret);
    if (!sizeof(ret)) return 0;
    if (sizeof(ret) == 1) return ret[0];
    return predef::`+(@ret);
}

int get_schema_version() {
    return sizeof(migrations);
}

array(object) get_migrations(int schema_version, mapping(string:int) type_versions) {
    if (schema_version < get_schema_version()) {
        return get_previous_schema(schema_version)->get_migrations(schema_version, type_versions)
            + migrations[schema_version..];
    }
    array(object) ret = ({ });
    // first get the type versions, then the schema migrations
    mapping my_versions = this_program::type_versions();

    this_program from;

    foreach (indices(type_versions + my_versions);; string type_name) {
        while (type_versions[type_name] < my_versions[type_name]) {
            if (!from)
                from = get_previous_schema(schema_version, type_versions);
            type_versions[type_name]++;
            this_program to = get_previous_schema(schema_version, type_versions);

            object migration = to->get_migration(type_name, from);

            ret += ({ migration });

            from = to;
        }
    }

    return ret;
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
    if (!this_program::m->version)
        add_type(version = SyncDB.Types.Version("version"));
}

mixed get_unique_identifier(mapping row) {
    return row->version;
}

object get_row_filter(mapping|object row) {
    object f = id->Equal(row[id->name]);
    return f;
}

object get_versioned_filter(mapping|object row) {
    object f = get_row_filter(row);
    if (version) {
        f &= version->Equal(row[version->name]);
    }
    return f;
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
    foreach (type->get_column_fields();; object type) {
        if (type->is->key) {
            if (id) error("Defined two different keys in one schema.\n");
            id = type;
        } else if (type->is->index) {
            index_list += ({ .Indices.Btree(type->name, type) });
        }
        if (type->is->automatic) {
            if (automatic) error("Defined two different auto-increment values in one schema\n");
            automatic = type->name;
        }
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
