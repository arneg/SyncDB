class Index {
    array(object) fields;
    string name;
    string type = "BTREE";

    void create(string name, object ... fields) {
        this_program::fields = fields;
        this_program::name = name;
    }

    array(string) column_names() {
        return Array.flatten(fields->sql_names());
    }

    array(string) escaped_column_names() {
        return Array.flatten(fields->escaped_sql_names());
    }

    .MySQL.Query create_definitions(void|string table_name) {
        string s;
        string constraint = "";

        if (has_value(fields->is->unique, 1)) {
            constraint = "UNIQUE";
        }

        if (table_name) {
            s = sprintf("CREATE %s INDEX `%s` USING %s ON `%s` (", constraint, name, type, table_name);
        } else {
            s = sprintf("%s INDEX `%s` USING %s (", constraint, name, type);
        }

        s += escaped_column_names() * ", " + ") " + option();

        return .MySQL.Query(s);
    }

    string option() {
        return "";
    }

    protected int(0..1) _equal(mixed o) {
        return objectp(o) && object_program(o) == this_program &&
                o->name == name && equal(o->fields, fields);
    }
}

class Btree {
    inherit Index;
}

class Hash {
    inherit Index;

    string type = "HASH";
}
