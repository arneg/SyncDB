void create_table(Sql.Sql sql, string name, object schema) {
    array a = ({ });

    int(0..1) filter_cb(object type) {
	return !type->is_foreign;
    };

    foreach (schema;; object type) if (!type->is_foreign) {
	werror("type: %O\n", type);
	string|array(string) tmp = type->sql_type(sql, filter_cb);
	a += arrayp(tmp) ? tmp : ({ tmp });
    }

    werror("a->sql_type() : %O\n", a);

    string s = sprintf("CREATE TABLE IF NOT EXISTS `%s` (", name);
    s += a * ", ";
    s += ")";

    werror("creating table: \n%s\n", s);
    sql->query(s);

    foreach (schema->index_fields;; object field) {
	int(0..1) uniq = field->is_unique;
	string q = sprintf("CREATE %s INDEX `%s` ON `%s` (%s)",
			   (uniq ? " UNIQUE " : ""), field->name, name);

	sql->query(q);
    }
}

