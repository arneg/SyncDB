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
    s += a * ", ";
    s += ")";

    sql->query(s);

    array(mapping) tmp = sql->query(sprintf("SHOW INDEX FROM `%s`", name));
    mapping mi = mkmapping(tmp->Column_name, allocate(sizeof(tmp), 1));

    foreach (schema->index_fields();; object field) {
	// already has index
	if (field->is_key) continue;
	if (mi[field->name]) continue;

	int(0..1) uniq = field->is_unique;
	string q = sprintf("CREATE %s INDEX `%s` ON `%s` (%s)",
			   (uniq ? " UNIQUE " : ""), field->name, name, field->name);

	sql->query(q);
    }
}
