class Or(array(object) ... filters) {

    string get_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + [array(string)]filters->get_sql(table) * " OR " + ")";
    }
}

class And(array(object) ... filters) {

    string get_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + [array(string)]filters->get_sql(table) * " AND " + ")";
    }
}

class Equal(string field, mixed|Serialization.Atom atom) {

    string get_sql(object table) {
	object o = atom;
	object type = table->m[field];
	if (Program.inherits(object_program(o), Serialization.Atom)) 
	    o = type->parser()->decode(o);
	return sprintf("%s = '%s'", table->get_sql_name(field), type->encode_sql(o));
    }
}

class True(string field) {
    string get_sql(object table) {
	return sprintf("%s IS NOT NULL", table->get_sql_name(field));
    }
}

class False(string field) {
    string get_sql(object table) {
	return sprintf("%s IS NULL", table->get_sql_name(field));
    }
}
