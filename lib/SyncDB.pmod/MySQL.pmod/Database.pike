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

object register_view(string name, object type) {
    object table = type->get_table(sqlcb, name);

    register_table(name, table);

    if (has_index(dependencies, name))
        foreach (dependencies[name]; string trigger; array(function) a)
            foreach (a;; function fun)
                table->register_trigger(trigger, fun);

    return table;
}

void unregister_view(string name, object type) {
    object table = low_get_table(name, type);
    unregister_table(name, table);
}

void register_table(string name, object table) {
    ::register_table(name, table);
    table->set_database(this);
}

void uregister_table(string name, object table) {
    table->set_database();
    ::unregister_table(name, table);
}

typedef function(object(SyncDB.Version),array(mapping|object):void) update_cb;

mapping(string:array(update_cb)) update_cbs = ([]);

void register_update(string table_name, update_cb cb) {
    update_cbs[table_name] += ({ cb });
}

void unregister_update(string table_name, update_cb cb) {
    if (has_index(update_cbs, table_name)) 
        update_cbs[table_name] -= ({ cb });
}

void signal_update(string|object table, object version, void|array(mapping) rows) {
    mapping t = all_tables();

    if (objectp(table)) {
        // local update, propagate to all tables globally
        string name = table->table_name();
        if (has_index(t, name)) (t[name] - ({ table }))->handle_update(version, rows);
        if (this_program::name) .signal_update(this, name, version, rows);
        if (has_index(update_cbs, name)) call_out(update_cbs[name], 0, version, rows);
    } else {
        if (has_index(t, table)) t[table]->handle_update(version, rows);
        if (has_index(update_cbs, table)) call_out(update_cbs[table], 0, version, rows);
    }
}
