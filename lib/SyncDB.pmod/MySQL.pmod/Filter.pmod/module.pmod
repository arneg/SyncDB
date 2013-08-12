
class Or(object ... filters) {

    object encode_sql(object table, function quote) {
	if (!sizeof(filters)) error("empty filter!");
	return SyncDB.MySQL.Query("(", filters->encode_sql(table, quote), " OR ") + ")";
    }

    string _sprintf(int type) {
	return sprintf("Or(%s)", filters->_sprintf('O')*", ");
    }
}

class And(object ... filters) {

    object encode_sql(object table, function quote) {
	if (!sizeof(filters)) error("empty filter!");
	return SyncDB.MySQL.Query("(", filters->encode_sql(table, quote), " AND ") + ")";
    }

    string _sprintf(int type) {
	return sprintf("And(%s)", filters->_sprintf('O')*", ");
    }
}

class Base {
    object type;
    mixed value;

    string `field() {
	return type->name;
    }

    void create(object type, mixed value) {
	this_program::type = type;
	this_program::value = value;
    }

    mapping `row() {
	return ([ field : value ]);
    }
}

class Equal {
    inherit Base;

    object encode_sql(object table) {
	mapping new = ([]);

	if (!type->is_index) // relieve this check for restrictions?
	    werror("Trying to index non-indexable field %O.\n", type);
	if (!type->is_readable)
	    error("Trying to index non-readable field.\n");
#if constant(Serialization)
	if (objectp(value) && Program.inherits(object_program(value), Serialization.Atom)) 
	    value = type->parser()->decode(value);
#endif
	type->encode_sql(table->table, row, new);
	return SyncDB.MySQL.Query("(", new, " = ", " AND ") + ")";
    }

    string _sprintf(int type) {
	return sprintf("Equal(%O, %O)", field, value);
    }

    void insert(object table, string name, function quote, mapping|void new) {
	if (!new) new = ([ ]);

	type->encode_sql(table->table, row, new);

	return new;
    }
}

class _In {
    inherit Base;

    object encode_sql(object table) {
        string l = "%s" + ",%s" * (sizeof(value) - 1);
        string fmt = sprintf("(%s in (%s))", type->sql_name(table->table), l);

	return SyncDB.MySQL.Query(fmt, @map(value, type->encode_sql_value));
    }

    string _sprintf(int type) {
	return sprintf("In(%O, %O)", field, value);
    }
}

object In(object type, array values) {
    if (!type->encode_sql_value) {
        return Or(@map(values, type->Equal));
    }

    return _In(type, values);
}

class Match {
    inherit Base;

    string _sprintf(int type) {
	return sprintf("Match(%O, %O)", field, value);
    }

    object encode_sql(object table) {
	mapping new = ([]);
	type->encode_sql(table->table, row, new);
	return SyncDB.MySQL.Query("(", new, " like ", " AND ") + ")";
    }
}

class Unary(object type, string op) {

    string `field() {
	return type->name;
    }

    object encode_sql(object table, function quote) {
	array a = type->sql_names(table->table);

	foreach (a; int i; mixed v) {
	    a[i] = sprintf("%s %s", v, op);
	}

	return SyncDB.MySQL.Query("(" + a * " AND " + ")");
    }

}

class True {
    inherit Unary;

    void create(object type) {
	::create(type, "IS NOT NULL");
    }

    string _sprintf(int type) {
	return sprintf("True(%O)", field);
    }
}

class False(string field) {
    inherit Unary;

    void create(object type) {
	::create(type, "IS NULL");
    }

    string _sprintf(int type) {
	return sprintf("False(%O)", field);
    }
}

// TODO: these should get the decoded value passed, instead of relying on 
// parser creation
#if constant(Serialization)
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

    string encode_sql(object table, function quote) {
	object range;
	range = parser(table)->decode(value);
	werror("Range: %O %O", object_program(range), range);

	return sprintf("(%s <= %s AND %s >= %s)",
		       start(table)->sql_name(table->table),
		       start(table)->encode_sql_value(range->stop, quote),
		       stop(table)->sql_name(table->table),
		       stop(table)->encode_sql_value(range->start, quote));
    }
    string _sprintf(int type) {
	return sprintf("Overlaps(%O)", field);
    }
}

class Contains {
    inherit RangeLookup;

    string encode_sql(object table, function quote) {
	object range;
	range = parser(table)->decode(value);

	werror("Range: %O %O", object_program(range), range);

	return sprintf("(%s >= %s AND %s <= %s)",
		       start(table)->sql_name(table->table),
		       start(table)->encode_sql_value(range->start, quote),
		       stop(table)->sql_name(table->table),
		       stop(table)->encode_sql_value(range->stop, quote));
    }
    string _sprintf(int type) {
	return sprintf("Overlaps(%O)", field);
    }
}

class Lt(string field, Serialization.Atom value) {
    string encode_sql(object table, function quote) {
	object type = table->schema[field];
	return sprintf("%s < %s", table->get_sql_name(field),
		       type->encode_sql_value(type->parser()->decode(value),
					      quote));
    }
    string _sprint(int type) {
	return sprintf("Lt(%O)", field);
    }
}

class Le(string field, Serialization.Atom value) {
    string encode_sql(object table, function quote) {
	object type = table->schema[field];
	return sprintf("%s <= %s", table->get_sql_name(field),
		       type->encode_sql_value(type->parser()->decode(value),
					      quote));
    }
    string _sprintf(int type) {
	return sprintf("Le(%O)", field);
    }
}

class Gt(string field, Serialization.Atom value) {
    string encode_sql(object table, function quote) {
	object type = table->schema[field];
	return sprintf("%s > %s", table->get_sql_name(field),
		       type->encode_sql_value(type->parser()->decode(value),
					      quote));
    }
    string _sprintf(int type) {
	return sprintf("Gt(%O)", field);
    }
}

class Ge(string field, Serialization.Atom value) {
    string encode_sql(object table, function quote) {
	object type = table->schema[field];
	return sprintf("%s >= %s", table->get_sql_name(field),
		       type->encode_sql_value(type->parser()->decode(value),
					      quote));
    }
    string _sprintf(int type) {
	return sprintf("Ge(%O)", field);
    }
}
#endif
