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

class RangeLookup(string field, Serialization.Atom value) {

    object parser(object table) {
	return master()->resolv("SyncDB.Serialization.Range")(start(table)->parser(), stop(table)->parser());
    }

    object start(object table) {
	object type = table->schema[field];
	if (type->fields && arrayp(type->fields)) {
	    return type->fields[0];
	} else return type;
    }

    object stop(object table) {
	object type = table->schema[field];
	if (type->fields && arrayp(type->fields)) {
	    return type->fields[1];
	} else return type;
    }
}

class Overlaps {
    inherit RangeLookup;

    string encode_sql(object table) {
	object range;
	range = parser(table)->decode(value);
	werror("Range: %O %O", object_program(range), range);

	return sprintf("(%s <= %s AND %s >= %s)",
		       start(table)->sql_name(table->table),
		       start(table)->encode_sql_value(range->stop),
		       stop(table)->sql_name(table->table),
		       stop(table)->encode_sql_value(range->start));
    }
    string _sprintf(int type) {
	return sprintf("Overlaps(%O)", field);
    }
}

class Contains {
    inherit RangeLookup;

    string encode_sql(object table) {
	object range;
	range = parser(table)->decode(value);

	werror("Range: %O %O", object_program(range), range);

	return sprintf("(%s >= %s AND %s <= %s)",
		       start(table)->sql_name(table->table),
		       start(table)->encode_sql_value(range->start),
		       stop(table)->sql_name(table->table),
		       stop(table)->encode_sql_value(range->stop));
    }
    string _sprintf(int type) {
	return sprintf("Overlaps(%O)", field);
    }
}

class Lt(string field, Serialization.Atom value) {
    string encode_sql(object table) {
	object type = table->schema[field];
	return sprintf("%s < %s", table->get_sql_name(field), type->encode_sql_value(type->parser()->decode(value)));
    }
    string _sprint(int type) {
	return sprintf("Lt(%O)", field);
    }
}

class Le(string field, Serialization.Atom value) {
    string encode_sql(object table) {
	object type = table->schema[field];
	return sprintf("%s <= %s", table->get_sql_name(field), type->encode_sql_value(type->parser()->decode(value)));
    }
    string _sprintf(int type) {
	return sprintf("Le(%O)", field);
    }
}

class Gt(string field, Serialization.Atom value) {
    string encode_sql(object table) {
	object type = table->schema[field];
	return sprintf("%s > %s", table->get_sql_name(field), type->encode_sql_value(type->parser()->decode(value)));
    }
    string _sprintf(int type) {
	return sprintf("Gt(%O)", field);
    }
}

class Ge(string field, Serialization.Atom value) {
    string encode_sql(object table) {
	object type = table->schema[field];
	return sprintf("%s >= %s", table->get_sql_name(field), type->encode_sql_value(type->parser()->decode(value)));
    }
    string _sprintf(int type) {
	return sprintf("Ge(%O)", field);
    }
}
