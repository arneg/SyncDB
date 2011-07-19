class Or(object ... filters) {

    string encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + filters->encode_sql(table) * " OR " + ")";
    }

    string _sprintf(int type) {
	return sprintf("Or(%s)", filters->_sprintf('O')*", ");
    }
}

class And(object ... filters) {

    string encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return "(" + filters->encode_sql(table) * " AND " + ")";
    }

    string _sprintf(int type) {
	return sprintf("And(%s)", filters->_sprintf('O')*", ");
    }
}

class Equal(string field, mixed value) {

    string encode_sql(object table) {
	mixed o = value;
	object type = table->schema[field];
	if (!type->is_index)
	    error("Trying to index non-indexable field.\n");
	if (!type->is_readable)
	    error("Trying to index non-readable field.\n");
	if (objectp(o) && Program.inherits(object_program(o), Serialization.Atom)) 
	    o = type->parser()->decode(o);
	return sprintf("%s=%s", table->get_sql_name(field), type->encode_sql_value(o));
    }

    string _sprintf(int type) {
	return sprintf("Equal(%O, %O)", field, value);
    }
}

class True(string field) {
    string encode_sql(object table) {
	return sprintf("%s IS NOT NULL", table->get_sql_name(field));
    }
    string _sprintf(int type) {
	return sprintf("True(%O)", field);
    }
}

class False(string field) {
    string encode_sql(object table) {
	return sprintf("%s IS NULL", table->get_sql_name(field));
    }
    string _sprintf(int type) {
	return sprintf("False(%O)", field);
    }
}

class Overlaps(string field, Serialization.Atom value) {
    string encode_sql(object table) {
	object range;
	object type = table->schema[field];
	if (!Program.inherits(object_program(type), SyncDB.Types.Range))
	    error("Overlaps requires a range.\n");
	range = type->parser()->decode(value);

	return sprintf("(%s <= %s AND %s >= %s)",
		       type->fields[0]->sql_name(table->table),
		       type->fields[0]->encode_sql_value(range->stop),
		       type->fields[1]->sql_name(table->table),
		       type->fields[1]->encode_sql_value(range->start));
    }
    string _sprintf(int type) {
	return sprintf("Overlaps(%O)", field);
    }
}
