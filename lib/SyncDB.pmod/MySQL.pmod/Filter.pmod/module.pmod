class Base {
    object And(object ... filters) {
        return master()->resolv("SyncDB.MySQL.Filter.And")(this, @filters);
    }

    object Or(object ... filters) {
        return master()->resolv("SyncDB.MySQL.Filter.Or")(this, @filters);
    }

    object Not() {
        return master()->resolv("SyncDB.MySQL.Filter.Not")(this);
    }

    mixed `&(mixed o) {
        if (o) return And(o);
        return this;
    }

    mixed ``&(mixed o) {
        if (o) return And(o);
        return this;
    }

    mixed `|(mixed o) {
        if (o) return Or(o);
        return this;
    }

    mixed ``|(mixed o) {
        if (o) return Or(o);
        return this;
    }

    void insert(mapping row) {
        werror("%O does not properly work as a restriction on insert.\n", this);
    }
}

class Combine {
    inherit Base;

    array(object) filters;

    void create(object ... filters) {
        array a = ({ });
        foreach (filters; int i; object o) {
            if (object_program(o) == this_program) {
                a += o->filters;
                filters[i] = 0;
            }
        }
        this_program::filters = filter(filters, filters) + a;
    }

    string _sprintf(int type) {
	return sprintf("%O(%s)", this_program, filters->_sprintf('O')*", ");
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && equal(filters, b->filters);
    }
}

class Or {
    inherit Combine;

    object encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return SyncDB.MySQL.Query("(", filters->encode_sql(table), " OR ") + ")";
    }
}

class And(object ... filters) {
    inherit Combine;

    object encode_sql(object table) {
	if (!sizeof(filters)) error("empty filter!");
	return SyncDB.MySQL.Query("(", filters->encode_sql(table), " AND ") + ")";
    }

    void insert(mapping row) {
        filters->insert(row);
    }
}

class FieldFilter {
    inherit Base;

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

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && type == b->type && value == b->value;
    }

    int(0..1) _equal(mixed b) {
        return objectp(b) && object_program(b) == this_program && type == b->type && equal(value, b->value);
    }
}

class BinaryFilter {
    inherit FieldFilter;

    string operator;

    object encode_sql(object table) {
	mapping new = ([]);

	if (!type->is_readable)
	    error("Trying to index non-readable field.\n");
#if constant(Serialization)
	if (objectp(value) && Program.inherits(object_program(value), Serialization.Atom)) 
	    value = type->parser()->decode(value);
#endif
	type->encode_sql(table->table_name(), row, new);
	return SyncDB.MySQL.Query("(", new, operator, " AND ") + ")";
    }
}

class Equal {
    inherit BinaryFilter;

    string operator = " = ";

    string _sprintf(int type) {
	return sprintf("Equal(%O, %O)", field, value);
    }

    void insert(mapping new) {
        new[field] = value;
    }
}

class Ne {
    inherit FieldFilter;

    object encode_sql(object table) {
	mapping new = ([]);

	if (!type->is_readable)
	    error("Trying to index non-readable field.\n");
#if constant(Serialization)
	if (objectp(value) && Program.inherits(object_program(value), Serialization.Atom)) 
	    value = type->parser()->decode(value);
#endif
	type->encode_sql(table->table_name(), row, new);
	return SyncDB.MySQL.Query("(", new, " != ", " OR ") + ")";
    }

    string _sprintf(int type) {
	return sprintf("Ne(%O, %O)", field, value);
    }
}

class Not(object filter) {
    inherit Base;

    object encode_sql(object table) {
	return "NOT (" + filter->encode_sql(table) + ")";
    }

    int(0..1) `==(mixed b) {
        return objectp(b) && object_program(b) == this_program && b->filter == filter;
    }
}

class _In {
    inherit FieldFilter;

    object encode_sql(object table) {
        string l = "%s" + ",%s" * (sizeof(value) - 1);
        string fmt = sprintf("(%s in (%s))", type->sql_name(table->table_name()), l);

	return SyncDB.MySQL.Query(fmt, @map(value, type->encode_sql_value));
    }

    string _sprintf(int type) {
	return sprintf("In(%O, %O)", field, value);
    }
}

object In(object type, array values) {
    if (!sizeof(values)) {
        return FALSE;
    }

    if (!type->encode_sql_value) {
        return Or(@map(values, type->Equal));
    }

    return _In(type, values);
}

class Match {
    inherit FieldFilter;

    string _sprintf(int type) {
	return sprintf("Match(%O, %O)", field, value);
    }

    object encode_sql(object table) {
	mapping new = ([]);
	type->encode_sql(table->table_name(), row, new);
	return SyncDB.MySQL.Query("(", new, " like ", " AND ") + ")";
    }
}

class Unary(object type, string op) {
    inherit Base;

    string `field() {
	return type->name;
    }

    object encode_sql(object table, function quote) {
	array a = type->sql_names(table->table_name());

	foreach (a; int i; mixed v) {
	    a[i] = sprintf("%s %s", v, op);
	}

	return SyncDB.MySQL.Query("(" + a * " AND " + ")");
    }

}

class Constant(SyncDB.MySQL.Query q) {
    inherit Base;

    object encode_sql(object table, function quote) {
        return q;
    }
}

object `FALSE() {
    return Constant(SyncDB.MySQL.Query("FALSE"));
}

object `TRUE() {
    return Constant(SyncDB.MySQL.Query("TRUE"));
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

class False {
    inherit Unary;

    void create(object type) {
	::create(type, "IS NULL");
    }

    string _sprintf(int type) {
	return sprintf("False(%O)", field);
    }

    void insert(mapping row) {
        row[field] = Val.null;
    }
}

class Gt {
    inherit BinaryFilter;

    string operator = " > ";

    string _sprintf(int type) {
	return sprintf("Gt(%O, %O)", field, value);
    }
}

class Ge {
    inherit BinaryFilter;

    string operator = " >= ";

    string _sprintf(int type) {
	return sprintf("Ge(%O, %O)", field, value);
    }
}

class Lt {
    inherit BinaryFilter;

    string operator = " < ";

    string _sprintf(int type) {
	return sprintf("Lt(%O, %O)", field, value);
    }
}

class Le {
    inherit BinaryFilter;

    string operator = " <= ";

    string _sprintf(int type) {
	return sprintf("Le(%O, %O)", field, value);
    }
}
