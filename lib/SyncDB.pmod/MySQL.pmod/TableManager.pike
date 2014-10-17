private mapping(string:array(object)) local_tables = ([]);

void unregister_view(string name, program type, object table) {
    if (!Program.inherits(type, .SmartType))
        werror("bad use of register_view(%O, %O, %O)\n", name, type, table);
    if (!local_tables[name]) return;
    local_tables[name] -= ({ table });
}

void register_table(string name, program type, object table) {
    if (!Program.inherits(type, .SmartType))
        werror("bad use of register_table(%O, %O, %O)\n", name, type, table);
    if (!local_tables[name]) {
        local_tables[name] = ({ });
    }
    local_tables[name] += ({ table });
}

mapping(string:array(object)) all_tables() {
    return local_tables;
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
        return sprintf("%O(%O, %O)", this_program, name, type);
    }
};

object low_get_table(string name, void|program type) {
    if (!local_tables[name] || !sizeof(local_tables[name]))
        return 0;

    if (!type) return local_tables[name][0];

    foreach (local_tables[name];; object table) {
        if (object_program(table->smart_type) == type) return table;
    }

    return 0;
}

object get_table(string name, void|program type) {
    return low_get_table(name, type) || RemoteTable(name, type);
}

function table_cb(string name, void|program type) {
    return Function.curry(get_table)(name, type);
}

void remove_all_tables() {
    local_tables = ([]);
}
