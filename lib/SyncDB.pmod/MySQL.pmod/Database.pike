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
    return table;
}

