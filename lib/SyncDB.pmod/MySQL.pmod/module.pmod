void create_table(Sql.Sql sql, string name, object schema) {
    array a = ({ });

    int(0..1) filter_cb(object type) {
	return !type->is->foreign;
    };

    if (has_value(sql->list_tables(), name)) {
        array current_fields = sql->list_fields(name);
        mapping fields = mkmapping(current_fields->name, current_fields);

        foreach (schema;; object type) if (!type->is->foreign) {
            string|array(string) tmp = type->sql_type(sql, filter_cb);
            array(string) names = type->sql_names("");
            names = filter(names, has_prefix, ".");
            if (!arrayp(tmp)) tmp = ({ tmp });
            foreach (names;int i; string name) {
                name = name[1..];
                if (has_index(fields, name)) continue;
                a += ({ tmp[i] });
            }
        }
        if (sizeof(a)) {
            string s = sprintf("ALTER TABLE `%s`", name);

            foreach (a;int i; string column_definition) {
                if (i) s += ", ";
                s += " ADD COLUMN " + column_definition;
            }
            .Query(s)(sql);
        }
    } else { /* table is completely new */
        foreach (schema;; object type) if (!type->is->foreign) {
            string|array(string) tmp = type->sql_type(sql, filter_cb);
            a += arrayp(tmp) ? tmp : ({ tmp });
        }

        string s = sprintf("CREATE TABLE IF NOT EXISTS `%s` (", name);
        s += a * ",";
        s += ")";

        .Query(s)(sql);
    }

    array(mapping) tmp = .Query(sprintf("SHOW INDEX FROM `%s`", name))(sql);
    mapping mi = mkmapping(tmp->Column_name, allocate(sizeof(tmp), 1));

    foreach (schema->index_fields();; object field) {
        // already has index
        if (field->is->key) continue;
        if (mi[field->name]) continue;

        int(0..1) uniq = field->is->unique;
        string q = sprintf("CREATE %s INDEX `%s` ON `%s` (`%s`)",
                           (uniq ? " UNIQUE " : ""), field->name, name, field->name);

        .Query(q)(sql);
    }
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
