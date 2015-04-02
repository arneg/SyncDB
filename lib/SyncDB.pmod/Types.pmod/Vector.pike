inherit .Base;

constant is_array = 1;

array(object) fields;

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

int(0..1) _equal(mixed b) {
    return ::_equal(b) && equal(fields, b->fields);
}

string type_name() {
    return "vector";
}
