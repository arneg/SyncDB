inherit "base";

void populate_table(Sql.Sql sql, string table_name, SyncDB.Schema schema) {
    object tbl = SyncDB.MySQL.Table(table_name, sql, schema);
    array rows = map(enumerate(150), Function.curry(sample_data)(schema));

    tbl->low_insert(rows);

    array a = tbl->low_select_complex();

    foreach (a; int i; mapping row) {
        m_delete(row, "version");
        m_delete(row, schema->automatic);
        if (!equal(row, schema->default_row + rows[i]))
            error("Data does not survive insert/select:\n%O vs %O\n", row, rows[i]);
    }
}

void test_migrations(object ... migrations) {
    SyncDB.Migration.Base(0, migrations[0]->from)->create_table("table1")(sql);
    populate_table(sql, "table1", migrations[0]->from);
    migrations->migrate(sql, "table1");

    migrations[-1]->create_table("table2")(sql);
    populate_table(sql, "table2", migrations[-1]->to);

    mixed err = catch {
        {
            mixed v1 = sql->query("SHOW CREATE TABLE table1")["Create Table"];
            mixed v2 = sql->query("SHOW CREATE TABLE table2")["Create Table"];

            // we do not care about the order of columns
            if (sizeof(v1) != 1 || sizeof(v2) != 1) {
                error("Cannot compare create table statements for %O %O\n", v1, v2);
            }

            v1 = sort(((v1[0]-",")/"\n")[1..]);
            v2 = sort(((v2[0]-",")/"\n")[1..]);

            if (!equal(v1, v2)) {
                error("Migrations %O failed: %O vs %O\n", migrations, v1, v2);
            }
        }

        {
            object tbl1 = SyncDB.MySQL.Table("table1", sql, migrations[-1]->to);
            object tbl2 = SyncDB.MySQL.Table("table2", sql, migrations[-1]->to);

            object it1 = tbl1->PageIterator(0, 0, 100);
            object it2 = tbl2->PageIterator(0, 0, 100);

            mapping mask = (migrations[0]->from->m & migrations[-1]->to->m) - ({ "version" });

            if (it1->num_rows() != it2->num_rows())
                error("Number of rows in tables differs after migration: %d vs %d\n",
                      it1->num_rows(), it2->num_rows());

            int cnt = 0;

            for (; it1 && it2; it1++, it2++) {
                array old = it1->value();
                array new = it2->value();

                foreach (old; int i; mapping v1) {
                    mapping v2 = new[i];

                    v2 = mask & v2;
                    v1 = mask & v1;
                    // just compare those values which have been present in both versions
                    if (!equal(v1, v2))
                        error("Content in tables differs after migration %O at %d.\n%O vs %O\n",
                              migrations, cnt, v1, v2);
                    cnt ++;
                }

            }
        }
    };

    migrations[0]->drop_table("table1")(sql);
    migrations[0]->drop_table("table2")(sql);

    if (err) throw(err);
}

void test_simple(SyncDB.Schema a, SyncDB.Schema b) {
    object mig = SyncDB.Migration.Simple(a, b);

    test_migrations(mig);
    
    array migrations = a->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
    migrations = b->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
}

void test_alter(SyncDB.Schema a, SyncDB.Schema b) {
    object mig = SyncDB.Migration.Base(a, b);

    test_migrations(mig);
    
    array migrations = a->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
    migrations = b->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
}

void test_smarttype(program type) {
    object a = type();
    array migrations = a->schema->get_migrations(0, ([]));
    
    if (sizeof(migrations))
        test_migrations(@migrations);
}

void _test1() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Datetime("bar"),
        SyncDB.Types.String("foo"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.String("bar"),
        SyncDB.Types.Datetime("foo"),
    );

    SyncDB.Migration.Base mig2 = SyncDB.Migration.Base(a, b);

    mig2->rename_type("bar", "foo");
    mig2->rename_type("foo", "bar");

    test_migrations(mig2);
}

void _test2() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Integer("bar"),
        SyncDB.Types.Integer("foo"),
        SyncDB.Types.Integer("flu"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.String("bar"),
        SyncDB.Types.String("foo"),
        SyncDB.Types.Integer("flu"),
    );

    test_migrations((class {
        inherit SyncDB.Migration.Simple;

        mapping transform_row(mapping row) {
            return ([
                "bar" : get_sample_data("string", row->bar),
                "foo" : get_sample_data("string", row->foo),
                "flu" : row->flu,
            ]);
        }
    })(a, b));
}

void _test3() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Integer("bar", SyncDB.Flags.Index()),
        SyncDB.Types.Integer("foo"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.String("bar"),
        SyncDB.Types.Integer("foo", SyncDB.Flags.Index()),
    );

    test_migrations((class {
        inherit SyncDB.Migration.Simple;

        mapping transform_row(mapping row) {
            return row + ([
                "bar" : get_sample_data("string", row->bar),
            ]);
        }
    })(a, b));
}

void _test4() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.String("bar"),
        SyncDB.Types.String("foo", 64),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.String("bar", 64),
        SyncDB.Types.String("foo"),
    );

    test_alter(a, b);
}

void _test5() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Datetime("bar"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.Date("bar"),
    );

    test_simple(a, b);
}

void _test6() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Integer("id", SyncDB.Flags.Key(), SyncDB.Flags.Automatic()),
        SyncDB.Types.Datetime("bar"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.Integer("id", SyncDB.Flags.Key(), SyncDB.Flags.Automatic()),
        SyncDB.Types.Date("bar"),
    );

    test_simple(a, b);
}

void _test61() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Integer("id", SyncDB.Flags.Key(), SyncDB.Flags.Automatic()),
        SyncDB.Types.Datetime("bar"),
        SyncDB.Types.Float("prio"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.Integer("id", SyncDB.Flags.Key(), SyncDB.Flags.Automatic()),
        SyncDB.Types.Date("bar"),
        SyncDB.Types.Float("prio"),
    );

    test_simple(a, b);
}

class A {
    inherit SyncDB.MySQL.SmartType;

    Field id = Integer(KEY, AUTOMATIC);
    Field foo = String();
    Field prio = Float();

    array(mapping(string:object)) changes = ({
        ([
            "foo" : 0,
            "bar" : Integer(),
        ]),
        ([
            "flu" : 0,
         ]),
        ([
            "flu" : JSON(DEFAULT(([]))),
        ]),
    });
};

void _test8() {
    object db = SyncDB.MySQL.Database(`sql, "migration");

    db->create_version_table();
    
    class B0 {
        inherit SyncDB.MySQL.SmartType;

        Field id = Integer(KEY, AUTOMATIC);
        Field bar = Integer();
    };

    class B1 {
        inherit SyncDB.MySQL.SmartType;

        Field id = Integer(KEY, AUTOMATIC);
        Field foo = String();

        array(mapping(string:object)) changes = ({
            ([
                "foo" : 0,
                "bar" : Integer(),
            ]),
        });
    };

    class B2 {
        inherit SyncDB.MySQL.SmartType;

        Field id = Integer(KEY, AUTOMATIC);
        Field foo = String();
        Field flu = JSON(DEFAULT(([])));

        array(mapping(string:object)) changes = ({
            ([
                "foo" : 0,
                "bar" : Integer(),
            ]),
            ([
                "flu" : 0,
             ]),
        });
    };

    class B3 {
        inherit SyncDB.MySQL.SmartType;

        Field id = Integer(KEY, AUTOMATIC);
        Field foo = String();

        array(mapping(string:object)) changes = ({
            ([
                "foo" : 0,
                "bar" : Integer(),
            ]),
            ([
                "flu" : 0,
             ]),
            ([
                "flu" : JSON(DEFAULT(([]))),
             ])
        });
    };

    array(object) types = ({ B0, B1, B2, B3 })();

    foreach (types;; object type) {
        db->register_view("table1", type);
        db->unregister_view("table1", type);
    }
}

void test_parallel_register(int(0..1) shared_db, string name, program ... progs) {
    object global_db;
    if (shared_db) global_db = SyncDB.MySQL.Database(`sql, "migration");
    if (!name) name = "table1";

    void do_migration(program prog) {
        object type = prog();
        object db = global_db || SyncDB.MySQL.Database(`sql, "migration");
        catch {
            db->create_version_table();
        };

            // NOTE: all threads try to create the version table, we dont care which one succeeds
        db->register_view(name, type);
        db->unregister_view(name, type);
    };

    foreach (progs;; program prog) {
        allocate(10, Thread.Thread)(do_migration, prog)->wait();
    }
}

void test_parallel_register_from_legacy(int(0..1) shared_db, string name, program ... types) {
    object type = types[0]();
    SyncDB.MySQL.create_table(sql, "table1", type->schema);
    // we know that legacy tables are broken
    catch (populate_table(sql, "table1", type->schema));
    test_parallel_register(shared_db, name, @types);
}

mapping(string:program|array(program)) smart_types() {
    return ([
            "a" : A,
            "b" : ({ B0, B1, B2, B3 })
            ]);
}

int main(int argc, array(string) argv) {
    string path = argv[1];

    mapping m = smart_types();

    foreach (sort(indices(m));; string n) {
        program|array(program) p = m[n];
        if (argc > 2) {
            if (!has_value(argv[2..], "smart_types") && !has_value(argv[2..], n)) continue;
        }

        if (arrayp(p)) {
            foreach (p; int i; program prog)
                run(path, sprintf("SmartType %s.%d (%O)", n, i, p[i]), test_smarttype, prog);
        } else {
            run(path, sprintf("SmartType %s (%O)", n, p), test_smarttype, p);
        }
    }

    foreach (sort(indices(m));; string n) {
        program|array(program) p = m[n];
        if (argc > 2) {
            if (!has_value(argv[2..], "register") && !has_value(argv[2..], n)) continue;
        }
        if (arrayp(p)) {
            run(path, sprintf("Register %s", n), test_parallel_register, 0, n, @p);
        } else {
            run(path, sprintf("Register %s", n), test_parallel_register, 0, n, p);
        }

        if (arrayp(p)) {
            run(path, sprintf("Shared db Register %s", n), test_parallel_register, 1, n, @p);
        } else {
            run(path, sprintf("Shared db Register %s", n), test_parallel_register, 1, n, p);
        }

        if (arrayp(p)) {
            run(path, sprintf("Shared db Register %s from legacy", n), test_parallel_register_from_legacy, 1, n, @p);
        } else {
            run(path, sprintf("Shared db Register %s from legacy", n), test_parallel_register_from_legacy, 1, n, p);
        }
    }

    return ::main(argc, argv);
}
