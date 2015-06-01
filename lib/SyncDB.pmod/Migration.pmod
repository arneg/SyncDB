class Base {
    .Schema from, to;

    string describe() {
        string ret = "Migrating ";
        if (from->get_schema_version() != to->get_schema_version()) {
            ret += sprintf("schema version from %d to %d", 
                           from->get_schema_version(),
                           to->get_schema_version());
        } else {
            // types changed
            mapping to_type_version = to->type_versions();
            mapping from_type_version = from->type_versions();

            int c = 0;

            foreach (sort(indices(to_type_version + from_type_version));; string name) {
                if (to_type_version[name] != from_type_version[name]) {
                    if (c) ret += ", ";
                    ret += sprintf("type %O from %d to %d", name,
                                   from_type_version[name],
                                   to_type_version[name]);
                    c++;
                }
            }

            if (!c) ret += "initial";
        }
        return ret;
    }

    string _sprintf(int t) {
        return sprintf("%O(%O, %O)", this_program, from, to);
    }

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

        statements += ({ .MySQL.Query("DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci") });

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

        statement += ") ENGINE InnoDB DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci";

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

            array(string) names = type->escaped_sql_names();

            foreach (names; int i; string column_name) {
                ret += ({ .MySQL.Query("DROP COLUMN " + column_name + "") });
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

            if (!t_from->schema_equal(t_to)) {
                array(string) from_names = t_from->escaped_sql_names();
                array(string) to_names = t_to->escaped_sql_names();
                array(.MySQL.Query) column_definitions = t_to->column_definitions();

                if (equal(column_definitions, t_from->column_definitions())) continue;

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

    void before_alter(Sql.Sql sql, string table_name);
    void after_alter(Sql.Sql sql, string table_name);

    mapping transform_row(mapping row);
    mapping update_row(mapping row);
    void update_table(object table);

    //! Perform the migration. @expr{sql@} is expected to hold a WRITE lock on the table.
    //! The lock will be gone when this function returns.
    void migrate(Sql.Sql sql, string table_name) {
        if (transform_row) {
            // TODO: handle renames
            mapping(string:int)|array(string) current_tables = sql->list_tables();

            current_tables = mkmapping(current_tables, allocate(sizeof(current_tables), 1));

            if (upgrade_table(table_name))
                error("Both %O and native table upgrades are not compatible.\n", transform_row);

            string cpy_table_name = table_name+"_migration_copy";
            string tmp_table_name = table_name+"_migration_tmp";

            if (current_tables[tmp_table_name]) {
                error("old migration tmp table still arround. please fix manually!\n");
            }

            if (current_tables[cpy_table_name]) {
                error("old migration cpy table still around. please fix manually!\n");
            }


            object tbl_orig = SyncDB.MySQL.Table(table_name, sql, from);
            
            create_table(cpy_table_name)(sql);

            object tbl = SyncDB.MySQL.Table(cpy_table_name, sql, to);

            object it = tbl_orig->PageIterator(0, 0, 103);

            foreach (it;; array|object rows) {
                rows = map((array)rows, transform_row);
                rows = filter(rows, rows);
                if (sizeof(rows)) tbl->low_insert(rows);
            }

            sql->query("UNLOCK TABLES;");

            sql->query(sprintf("RENAME TABLE `%s` TO `%s`, `%s` TO `%s`",
                               table_name, tmp_table_name,
                               cpy_table_name, table_name));

            drop_table(tmp_table_name)(sql);
        } else {
            if (before_alter) before_alter(sql, table_name);
            .MySQL.Query alter = upgrade_table(table_name);
            if (alter) alter(sql);

            object tbl = SyncDB.MySQL.Table(table_name, sql, to);

            if (update_table) {
                update_table(tbl);
            } else if (update_row) {
                // fetch all rows and update
                foreach (tbl->PageIterator(0, 0, 100);; array|object rows) {
                    foreach (rows;; mapping row) {
                        row = update_row(row);
                        if (row) tbl->update(row, row->version, lambda(int n, mixed ... bar) {});
                    }
                }
            }
            if (after_alter) after_alter(sql, table_name);

            sql->query("UNLOCK TABLES;");
        }
    }
}

class Simple {
    inherit Base;

    mapping transform_row(mapping row) {
        return row;
    }

    .MySQL.Query upgrade_table() {
        // These transformation are actually not going to be needed, we are
        // creating a new table anyway, reinserting everything.
        return 0;
    }

    this_program `+(this_program ... b) {
        if (sizeof(map(b, object_program) - ({ this_program })))
            error("Bad argument.\n");
        return this;
    }
}
