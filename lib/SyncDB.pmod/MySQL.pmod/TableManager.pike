private mapping(string:array(object)) local_tables = ([]);

object unregister_table(string name, object table) {
    if (!local_tables[name]) return 0;
    local_tables[name] -= ({ table });

    return table;
}

object register_table(string name, object table) {
    if (!local_tables[name])
        local_tables[name] = ({ });
    local_tables[name] += ({ table });

    return table;
}

mapping(string:array(object)) all_tables() {
    return local_tables;
}

class RemoteTable(string name, void|object|program type) {
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
        return sprintf("%O(%O, %O)", this_program, name, type);
    }
};

object low_get_table(string name, void|program|object type) {
    if (!local_tables[name] || !sizeof(local_tables[name]))
        return 0;

    if (!type) return local_tables[name][0];

    foreach (local_tables[name];; object table) {
        if (!type || (objectp(type) && table->smart_type == type) ||
            (programp(type) && object_program(table->smart_type) == type)) return table;
    }

    return 0;
}

object get_table(string name, void|object|program type) {
    return low_get_table(name, type) || RemoteTable(name, type);
}

function table_cb(string name, void|object type) {
    return Function.curry(get_table)(name, type);
}

void remove_all_tables() {
    local_tables = ([]);
}
