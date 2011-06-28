inherit .Base;

constant is_array = 1;

array(object) fields;

void create(string name, array(object) fields, SyncDB.Flags.Base ... flags) {
    this_program::fields = fields;
    ::create(name, @flags);
}

mapping encode_sql(string table, mapping row, void|mapping new) {
    if (!new) new = ([]);
    if (has_index(row, name)) {
	mixed a = row[name];
	if (!arrayp(a) || sizeof(a) != sizeof(fields))
	    error("Type mismatch.\n");
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
	    return UNDEFINED;
    }
    if (new) new[name] = ret;
    return ret;
}

object get_parser() {
    return Serialization.Types.Tuple(@fields->get_parser());
}

array(string) sql_names(string table) {
    return `+(@fields->sql_names(table));
}

string encode_json(void|string p, void|array extra) {
    return ::encode_json(p||"SyncDB.Types.Vector", 
			 extra||map(fields, Standards.JSON.encode));
}
