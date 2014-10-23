inherit .TableManager;

function sqlcb;
string name;


// list of tables keeping references to table 'trigger'+'name'
mapping(string:mapping(string:array(function))) dependencies = ([]);

void create(function sqlcb, void|string name) {
    this_program::sqlcb = sqlcb;
    this_program::name = name;
    if (name) .register_database(name, this);
}

void destroy() {
    if (name) .unregister_database(name, this);
}

//! register a trigger from a remote table
void register_dependency(string table, string trigger, function fun) {
    if (!dependencies[table])
        dependencies[table] = ([]);

    if (!dependencies[table][trigger])
        dependencies[table][trigger] = ({});

    dependencies[table][trigger] += ({ fun });
}

void unregister_dependency(string table, string trigger, function fun) {
    dependencies[table][trigger] -= ({ fun });
}

object register_view(string name, program type) {
    object table = type()->get_table(sqlcb, name);
    register_table(name, type, table);
    table->set_database(this);

    if (has_index(dependencies, name))
        foreach (dependencies[name]; string trigger; array(function) a)
            foreach (a;; function fun)
                table->register_trigger(trigger, fun);

    return table;
}

void signal_update(string|object table, object version, void|array(mapping) rows) {
    mapping t = all_tables();

    if (objectp(table)) {
        // local update, propagate to all tables globally
        string name = table->table_name();
        if (has_index(t, name)) (t[name] - ({ table }))->handle_update(version, rows);
        if (this_program::name) .signal_update(this, name, version, rows);
    } else {
        if (has_index(t, table)) t[table]->handle_update(version, rows);
    }
}

void unregister_table(object table) {
    unregister_view(table->table_name(), object_program(table->smart_type), table); 
}
