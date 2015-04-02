.Schema from, to;

mapping from_types() {
    return from ? from->m : ([]);
}

mapping to_types() {
    return to ? to->m : ([]);
}

mapping from_indices() {
    return from ? mkmapping(from->get_indices()->name, from->get_indices()) : ([]);
}

mapping to_indices() {
    return to ? mkmapping(to->get_indices()->name, to->get_indices()) : ([]);
}

private mapping _rename_types = ([]);

void rename_type(string old_name, string new_name) {
    if (!has_index(from_types(), old_name))
        error("Unknown field %O\n", old_name);
    if (!has_index(to_types(), new_name))
        error("Unknown field %O\n", new_name);

    _rename_types[old_name] = new_name;
}

mapping rename_types() {
    return _rename_types;
}

mapping ignore_from() {
    return ([ ]);
}

mapping ignore_to() {
    return ([ ]);
}

/* different trivial changes, that should be handled automatically
 *
 * - deleted columns
 * - columns changing type trivially (so that mysql does it without problems)
 * - columns being added
 * - creation/deletion and change of indices
 *
 * things that require manual handling
 *
 * - renaming (should be simple)
 * - other harder transformations
 */

void create(.Schema from, .Schema to) {
    this_program::to = to;
    this_program::from = from;
}

.MySQL.Query upgrade_table(string table_name) {

    .MySQL.Query alter = .MySQL.Query("ALTER TABLE `"+table_name+"` ");

    array(.MySQL.Query) statements = alter_statements();

    if (!sizeof(statements)) return 0;

    foreach (statements; int i; object q) {
        if (i) alter += ",";
        alter += q;
    }

    return alter;
}

.MySQL.Query create_table(string table_name) {
    array(.MySQL.Query) definitions = predef::`+(@to->fields->column_definitions());

    .MySQL.Query statement = .MySQL.Query("CREATE TABLE `"+table_name+"` (");

    definitions += values(to_indices())->create_definitions();

    foreach (definitions; int i; .MySQL.Query definition) {
        if (i) statement += ",";
        statement += definition;
    }

    statement += ")";

    return statement;
}

.MySQL.Query drop_table(string table_name) {
    return .MySQL.Query("DROP TABLE `"+table_name+"`");
}

array(.MySQL.Query) alter_statements() {
    if (!from) return ({ });
    return drop_columns() + add_columns() + rename_columns() + drop_indices() + add_indices() + modify_columns();
}

array(.MySQL.Query) add_columns() {
    mapping to_add = (to_types() - values(rename_types())) - from_types() - ignore_to();

    array(.MySQL.Query) ret = ({ });

    foreach (to;; object type) {
        if (!to_add[type->name]) continue;

        array(.MySQL.Query) column_definitions = type->column_definitions();

        foreach (column_definitions; int i; .MySQL.Query column_definition) {
            ret += ({ "ADD COLUMN " + column_definition });
        }
    }

    return ret;
}

array(.MySQL.Query) drop_columns() {
    mapping to_drop = (from_types() - indices(rename_types())) - to_types() - ignore_from();

    array(.MySQL.Query) ret = ({ });

    if (from) foreach (from;; object type) {
        if (!to_drop[type->name]) continue;

        array(string) names = type->sql_names();

        foreach (names; int i; string column_name) {
            ret += ({ .MySQL.Query("DROP COLUMN `" + column_name + "`") });
        }
    }

    return ret;
}

array(.MySQL.Query) rename_columns() {
    mapping a = from_types();
    mapping b = to_types();
    mapping rename = rename_types();

    array(.MySQL.Query) ret = ({ });

    if (from) foreach (from;; object type) {
        string new_name = rename[type->name];
        if (!new_name) continue;
        object new_type = b[new_name];

        array(.MySQL.Query) column_definitions = new_type->column_definitions();

        foreach (type->escaped_sql_names(); int i; string old_name)
            ret += ({ .MySQL.Query(sprintf("CHANGE COLUMN %s ", old_name)) + column_definitions[i] });
    }

    return ret;
}

array(.MySQL.Query) modify_columns() {
    mapping to_modify = (from_types() - indices(rename_types())) & (to_types() - values(rename_types()));

    array(.MySQL.Query) ret = ({ });

    foreach (to_modify; string name; object type) {
        object t_from = from_types()[name];
        object t_to = to_types()[name];

        if (!equal(t_from, t_to)) {
            array(string) from_names = t_from->escaped_sql_names();
            array(string) to_names = t_to->escaped_sql_names();
            array(.MySQL.Query) column_definitions = t_to->column_definitions();

            if (!equal(from_names, to_names)) {
                werror("Cannot automatically transform %O to %O\n", t_from, t_to);
                continue;
            }
            foreach (to_names; int i; string name)
                ret += ({ .MySQL.Query(sprintf("CHANGE COLUMN %s ", name)) + column_definitions[i] });
        }
    }

    return ret;
}

array(.MySQL.Query) drop_indices() {
    mapping to_drop = from_indices() - to_indices();

    return map(map(indices(to_drop), Function.curry(sprintf)("DROP INDEX `%s`")), .MySQL.Query);
}

array(.MySQL.Query) add_indices() {
    mapping to_add = to_indices() - from_indices();

    array(.MySQL.Query) ret = ({ });

    if (sizeof(to_add)) foreach (to_indices();; object index) {
        if (!to_add[index->name]) continue;
        ret += ({ "ADD " + index->create_definitions() });
    }

    return ret;
}

void migrate(Sql.Sql sql, string table_name) {
    upgrade_table(table_name)(sql); 
}
