inherit .Base;

constant is_array = 1;

array(object) fields;

array(this_program) get_column_fields() {
    return predef::`+(@fields->get_column_fields());
}

mapping `subfields() {
    return mkmapping(fields->name, fields);
}

void create(string name, array(object) fields, SyncDB.Flags.Base ... flags) {
    this_program::fields = fields;
    ::create(name, @flags);
}

mapping encode_sql(string table, mapping row, void|mapping new) {
    if (!new) new = ([]);
    if (has_index(row, name)) {
	mixed a = row[name];
	if (!arrayp(a) || sizeof(a) != sizeof(fields))
	    error("Type mismatch. %O\n", a);
	foreach (fields; int i; object type) {
	    type->encode_sql(table, ([ type->name : a[i] ]), new);
	}
    }
    return new;
}

mixed decode_sql(string table, mapping row, void|mapping new) {
    array ret = allocate(sizeof(fields));
    foreach (fields; int i; object type) {
	if (zero_type(ret[i] = type->decode_sql(table, row)))
	    return new ? new : UNDEFINED;
    }
    if (new) new[name] = ret;
    return ret;
}

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.Tuple("_vector", 0, @fields->get_parser());
}
#endif

array(string) sql_names(string table) {
    return `+(@fields->sql_names(table));
}

array(string) escaped_sql_names(string table) {
    return `+(@fields->escaped_sql_names(table));
}

string encode_json(void|string p, void|array extra) {
    return ::encode_json(p||"SyncDB.Types.Vector", 
			 extra||map(fields, Standards.JSON.encode));
}

array(string)|string sql_type(Sql.Sql sql, void|function(object:int(0..1)) filter_cb) {
    return (filter_cb ? filter(fields, filter_cb) : fields)->sql_type(sql);
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    return predef::`+(@((filter_cb ? filter(fields, filter_cb) : fields)->column_definitions()));
}

int(0..1) schema_equal(mixed b) {
    if (!::schema_equal(b)) return 0;

    foreach (fields; int i; object t) {
        if (!t->schema_equal(b->fields[i])) return 0;
    }

    return 1;
}

int(0..1) _equal(mixed b) {
    return ::_equal(b) && equal(fields, b->fields);
}

string type_name() {
    return "vector";
}

void type_versions(mapping(string:int) versions) {
    ::type_versions(versions);
    fields->type_versions(versions);
}

object get_previous_type(string type_name, int version) {
    if (this_program::type_name() == type_name) {
        if (type_version() > version) return previous_type()->get_previous_type(type_name, version);
        else return this;
    } else return this_program(name, fields->get_previous_type(type_name, version), @_flags);
}

object get_migration(string type_name, object from, object to) {
    array(object) ret = fields->get_migration(type_name, from, to) +
                        ({ ::get_migration(type_name, from, to) });

    ret = filter(ret, ret);
    if (!sizeof(ret)) return 0;
    if (sizeof(ret) == 1) return ret[0];
    return predef::`+(@ret);
}

string _sprintf(int t) {
    string f = map(_flags, Function.curry(sprintf)("%O")) * ", ";
    if (sizeof(f)) f = ", "+f;
    return sprintf("%O(%O, %O%s)", this_program, name, fields, f);
}

int(0..1) supports_native_default() {
    foreach (fields;; object f) {
        if (!f->supports_native_default()) return 0;
    }

    return 1;
}

void add_solr_field_types(mapping types) {
    fields->add_solr_field_types(types);
}
void add_solr_fields(mapping f, void|mapping field_defaults) {
    fields->add_solr_fields(f, field_defaults);
}
