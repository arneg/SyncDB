private mapping(string:array(object)) tables = ([]);

void unregister_view(string name, program type, object table) {
    werror("unregistering table %O %O\n", name, table);
    tables[name] -= ({ table });
}

void register_table(string name, program type, object table) {
    if (!tables[name]) {
        tables[name] = ({ });
    }
    tables[name] += ({ table });
    werror("registering table %O %O\n", name, table);
}

mapping(string:array(object)) all_tables() {
    return tables;
}

class RemoteTable(string name, void|program type) {
    object table;

    mixed `->(string key) {
        if (!table) {
            table = low_get_table(name, type);
            if (!table) error("table %O not available.\n", name);
        }
        return predef::`->(table, key);
    }

    mixed `->=(string key, mixed value) {
        if (!table) {
            table = low_get_table(name, type);
            if (!table) error("table %O not available.\n", name);
        }
        return predef::`->=(table, key, value);
    }

    string _sprintf(int t) {
        return sprintf("%O(%s)", this_program, name);
    }
};

object low_get_table(string name, void|program type) {
    if (!tables[name] || !sizeof(tables[name]))
        return 0;

    if (!type) return tables[name][0];

    foreach (tables[name];; object table) {
        if (table->prog == type) return table;
    }

    return 0;
}

object get_table(string name, void|program type) {
    return low_get_table(name, type) || RemoteTable(name, type);
}

function table_cb(string name, void|program type) {
    return Function.curry(get_table)(name, type);
}

