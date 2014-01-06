inherit .TableManager;

function sqlcb;

// list of tables keeping references to table 'trigger'+'name'
mapping(string:mapping(string:array(function))) dependencies = ([]);

void create(function sqlcb) {
    this_program::sqlcb = sqlcb;
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

void signal_update(object table, object version, void|array(mapping) rows) {
    mapping t = all_tables();
    string name = table->table_name();

    if (has_index(t, name)) (t[name] - ({ table }))->handle_update(version, rows);
}

void unregister_table(object table) {
    unregister_view(table->table_name(), table->prog, table); 
}
