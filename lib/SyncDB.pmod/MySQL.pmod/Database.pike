inherit .TableManager;

function sqlcb;

mapping(string:array(function)) dependencies = ([]);

void create(function sqlcb) {
    this_program::sqlcb = sqlcb;
}
//! @[table_name1] depends on @[table_name2]
void register_dependency(string table_name1, string table_name2, function check) {
}

object register_view(string name, program type) {
    object table = type()->get_table(sqlcb, name);
    register_table(name, type, table);
    table->update_manager = this;
    return table;
}

void signal_update(object table, object version, void|array(mapping) rows) {
    mapping t = all_tables();
    string name = table->table_name();

    if (has_index(t, name)) (t[name] - ({ table }))->signal_update(version, rows);
}

void unregister_table(object table) {
    unregister_view(table->table_name(), table->prog, table); 
}
