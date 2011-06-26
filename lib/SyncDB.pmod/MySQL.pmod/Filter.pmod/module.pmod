class Or(object ... filters) {

    string encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + filters->encode_sql(table) * " OR " + ")";
    }
}

class And(object ... filters) {

    string encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + filters->encode_sql(table) * " AND " + ")";
    }
}

class Equal(string field, mixed atom) {

    string encode_sql(object table) {
	mixed o = atom;
	object type = table->schema[field];
	if (!type->is_index)
	    error("Trying to index non-indexable field.");
	if (!type->is_readable)
	    error("Trying to index non-readable field.");
	if (objectp(o) && Program.inherits(object_program(o), Serialization.Atom)) 
	    o = type->parser()->decode(o);
	return sprintf("%s=%s", table->get_sql_name(field), type->encode_sql_value(o));
    }
}

class True(string field) {
    string encode_sql(object table) {
	return sprintf("%s IS NOT NULL", table->get_sql_name(field));
    }
}

class False(string field) {
    string encode_sql(object table) {
	return sprintf("%s IS NULL", table->get_sql_name(field));
    }
}
