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
        if (objectp(o)) return And(o);
        if (o) error("Bad argument.\n");
        return this;
    }

    mixed ``&(mixed o) {
        if (objectp(o)) return And(o);
        if (o) error("Bad argument.\n");
        return this;
    }

    mixed `|(mixed o) {
        if (objectp(o)) return Or(o);
        if (o) error("Bad argument.\n");
        return this;
    }

    mixed ``|(mixed o) {
        if (objectp(o)) return Or(o);
        if (o) error("Bad argument.\n");
        return this;
    }

    void insert(mapping row) { }

    array get_all_field_values(string field) {
        return 0;
    }

    int(-1..1) test(mapping|object row) {
        return -1;
    }
}

class Combine {
    inherit Base;

    array(object) filters;

    void create(object ... filters) {
        array a = ({ });
        foreach (filters; int i; object o) {
            if (!objectp(o)) error("Bad argument.\n");
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

    int(-1..1) test(mapping|object row) {
        foreach (filters;; object f) {
            if (f->test(row)) return 1;
        }

        return 0;
    }

    array get_all_field_values(string field) {
        array a = filters->get_all_field_values(field);

        // There is one which is not compatible
        if (Array.any(a, `!)) return 0;

        return predef::`+(@a);
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

    int(-1..1) test(mapping|object row) {
        foreach (filters;; object f) {
            if (!f->test(row)) return 0;
        }

        return 1;
    }

    array get_all_field_values(string field) {
        array a = filters->get_all_field_values(field);

        // There is one which is not compatible
        a = filter(a, arrayp);

        if (!sizeof(a)) return 0;

        return predef::`+(@a);
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

    int(-1..1) _equal(mixed b) {
        return objectp(b) && object_program(b) == this_program && type == b->type && equal(value, b->value);
    }
}

class BinaryFilter {
    inherit FieldFilter;

    string operator;

    object encode_sql(object table) {
	mapping new = ([]);

	if (!type->is->readable)
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

    int(-1..1) test(mapping|object row) {
        return row[field] == value;
    }

    array get_all_field_values(string field) {
        if (field == this_program::field) {
            return ({ value });
        } else {
            return 0;
        }
    }
}

class Ne {
    inherit FieldFilter;

    object encode_sql(object table) {
	mapping new = ([]);

	if (!type->is->readable)
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

    int(-1..1) test(mapping|object row) {
        return row[field] != value;
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

    int(-1..1) test(mapping|object row) {
        return !filter->test(row);
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

    int(-1..1) test(mapping|object row) {
        return has_value(value, row[field]);
    }

    array get_all_field_values(string field) {
        if (field == this_program::field) {
            return value + ({ });
        } else {
            return 0;
        }
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

class _FALSE {
    inherit Constant;

    void create() {
        ::create(SyncDB.MySQL.Query("FALSE"));
    }

    int(-1..1) test(mapping|object row) {
        return 1;
    }
}

class _TRUE {
    inherit Constant;

    void create() {
        ::create(SyncDB.MySQL.Query("TRUE"));
    }

    int(-1..1) test(mapping|object row) {
        return 0;
    }
}

object FALSE = _FALSE();
object TRUE = _TRUE();

class True {
    inherit Unary;

    void create(object type) {
	::create(type, "IS NOT NULL");
    }

    string _sprintf(int type) {
	return sprintf("True(%O)", field);
    }

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return !objectp(v) || !v->is_val_null;
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

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return objectp(v) && v->is_val_null;
    }
}

class Gt {
    inherit BinaryFilter;

    string operator = " > ";

    string _sprintf(int type) {
	return sprintf("Gt(%O, %O)", field, value);
    }

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return v > value;
    }
}

class Ge {
    inherit BinaryFilter;

    string operator = " >= ";

    string _sprintf(int type) {
	return sprintf("Ge(%O, %O)", field, value);
    }

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return v >= value;
    }
}

class Lt {
    inherit BinaryFilter;

    string operator = " < ";

    string _sprintf(int type) {
	return sprintf("Lt(%O, %O)", field, value);
    }

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return v < value;
    }
}

class Le {
    inherit BinaryFilter;

    string operator = " <= ";

    string _sprintf(int type) {
	return sprintf("Le(%O, %O)", field, value);
    }

    int(-1..1) test(mapping|object row) {
        mixed v = row[field];
        return v <= value;
    }
}
