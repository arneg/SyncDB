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

        if (table_name) {
            s = sprintf("CREATE INDEX `%s` USING %s ON `%s` (", name, type, table_name);
        } else {
            s = sprintf("INDEX `%s` USING %s (", name, type);
        }

        s += escaped_column_names() * ", " + ") " + option();

        return .MySQL.Query(s);
    }

    string option() {
        return "";
    }
}

class Btree {
    inherit Index;
}

class Hash {
    inherit Index;

    string type = "HASH";
}
