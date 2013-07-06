void create_table(Sql.Sql sql, string name, object schema) {
    array a = ({ });

    int(0..1) filter_cb(object type) {
	return !type->is_foreign;
    };

    foreach (schema;; object type) if (!type->is_foreign) {
	werror("type: %O\n", type);
	string|array(string) tmp = type->sql_type(filter_cb);
	a += arrayp(tmp) ? tmp : ({ tmp });
    }

    werror("a->sql_type() : %O\n", a);

    string s = "CREATE TABLE IF NOT EXISTS " + name + " (";
    s += a * ", ";
    s += ")";

    werror("creating table: \n%s\n", s);
    sql->query(s);
}

