void create_table(Sql.Sql sql, string name, object schema) {
    array a = ({ });

    int(0..1) filter_cb(object type) {
	return !type->is_foreign;
    };

    foreach (schema;; object type) if (!type->is_foreign) {
	string|array(string) tmp = type->sql_type(sql, filter_cb);
	a += arrayp(tmp) ? tmp : ({ tmp });
    }

    string s = sprintf("CREATE TABLE IF NOT EXISTS `%s` (", name);
    s += a * ",";
    s += ")";

    .Query(s)(sql);

    array(mapping) tmp = .Query(sprintf("SHOW INDEX FROM `%s`", name))(sql);
    mapping mi = mkmapping(tmp->Column_name, allocate(sizeof(tmp), 1));

    foreach (schema->index_fields();; object field) {
	// already has index
	if (field->is_key) continue;
	if (mi[field->name]) continue;

	int(0..1) uniq = field->is_unique;
	string q = sprintf("CREATE %s INDEX `%s` ON `%s` (`%s`)",
			   (uniq ? " UNIQUE " : ""), field->name, name, field->name);

        .Query(q)(sql);
    }
}

mapping(program:object) type_to_schema = ([]);
mapping(program:array(object)) type_to_fields = ([]);
mapping(program:mapping(string:object)) type_to_nfields = ([]);

object get_schema(program type) {
    return type_to_schema[type];
}

void set_schema(program type, object schema) {
    type_to_schema[type] = schema;
}

array(object) get_fields(program type) {
    return type_to_fields[type];
}

mapping(string:object) get_nfields(program type) {
    return type_to_nfields[type];
}

void set_fields(program type, array(object) fields) {
    type_to_fields[type] = fields;
    type_to_nfields[type] = mkmapping(fields->name, fields);
}

mapping(string:array(object)) all_databases = ([]);

void register_database(string name, object db) {
    if (!has_index(all_databases, name)) all_databases[name] = ({});
    all_databases[name] += ({ db });
}

void unregister_database(string name, object db) {
    if (has_index(all_databases, name))
        all_databases[name] -= ({ db });
}

void signal_update(object db, string table, object version, void|array rows) {
    string name = db->name;
    (all_databases[name] - ({ db }))->signal_update(table, version, rows);
}
