inherit "base";

void populate_table(Sql.Sql sql, string table_name, SyncDB.Schema schema) {
    object tbl = SyncDB.MySQL.Table(table_name, sql, schema);

    tbl->low_insert(map(enumerate(100), Function.curry(sample_data)(schema)));
}

void test_migrations(object ... migrations) {
    SyncDB.Migration(0, migrations[0]->from)->create_table("table1")(sql);
    populate_table(sql, "table1", migrations[0]->from);
    migrations->migrate(sql, "table1");

    migrations[-1]->create_table("table2")(sql);
    populate_table(sql, "table2", migrations[-1]->to);

    mixed err = catch {
        {
            mixed v1 = sql->query("SHOW CREATE TABLE table1")["Create Table"];
            mixed v2 = sql->query("SHOW CREATE TABLE table1")["Create Table"];

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

            object it1 = tbl1->PageIterator(0, 0, 1);
            object it2 = tbl2->PageIterator(0, 0, 1);

            if (it1->num_rows() != it2->num_rows())
                error("Number of rows in tables differs after migration: %d vs %d\n", it1->num_rows(), it2->num_rows());

            for (; it1 && it2; it1++, it2++) {
                if (!equal(it1->value(), it2->value()))
                    error("Content in tables differs after migration.\n%O vs %O\n", it1->value(), it2->value());
            }
        }
    };

    migrations[0]->drop_table("table1")(sql);
    migrations[0]->drop_table("table2")(sql);

    if (err) throw(err);
}

void test_simple(SyncDB.Schema a, SyncDB.Schema b) {
    // FIXME: this aborts pike
    // object(inherits SyncDB.Migration) mig = SyncDB.SimpleMigration(a, b);
    object mig = SyncDB.SimpleMigration(a, b);

    test_migrations(mig);
    
    array migrations = a->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
    migrations = b->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
}

void test_alter(SyncDB.Schema a, SyncDB.Schema b) {
    SyncDB.Migration mig = SyncDB.Migration(a, b);

    test_migrations(mig);
    
    array migrations = a->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
    migrations = b->get_migrations(0, ([]));
    if (sizeof(migrations)) test_migrations(@migrations);
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

    SyncDB.Migration mig2 = SyncDB.Migration(a, b);

    mig2->rename_type("bar", "foo");
    mig2->rename_type("foo", "bar");

    test_migrations(mig2);
}

void _test2() {
    SyncDB.Schema a = SyncDB.Schema(
        SyncDB.Types.Integer("bar"),
        SyncDB.Types.Integer("foo"),
    );

    SyncDB.Schema b = SyncDB.Schema(
        SyncDB.Types.String("bar"),
        SyncDB.Types.String("foo"),
    );

    test_migrations((class {
        inherit SyncDB.SimpleMigration;

        mapping transform_row(mapping row) {
            return ([
                "bar" : (string)row->bar,
                "foo" : (string)row->foo,
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
        inherit SyncDB.SimpleMigration;

        mapping transform_row(mapping row) {
            return row + ([
                "bar" : (string)row->bar,
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

int success_count, error_count;

void run(string path, function(mixed...:void) r, mixed ... args) {
    sql_path = path;

    catch (sql->query("DROP DATABASE `migration_test`"));

    sql->query("CREATE DATABASE `migration_test`");

    sql_path = path + "migration_test";

    werror("Running test %O ... ", r);

    mixed err;

    int t1 = gethrtime();
    float t = gauge {
        err = catch(r(@args));
    };
    int t2 = gethrtime();
    float t_tot = (t2 - t1)/ 1E6;

    if (err) {
        werror(" ERR %f seconds (utime: %f seconds)\n", t_tot, t);
        error_count++;
        master()->handle_error(err);
    } else {
        werror("  OK %f seconds (utime: %f seconds)\n", t_tot, t);
        success_count++;
    }
}

int main(int argc, array(string) argv) {
    foreach (sort(indices(this));; string s) {
        if (has_prefix(s, "_test")) {
            mixed v = predef::`->(this, s);
            if (functionp(v)) run(argv[-1], v);
        }
    }

    int all = success_count + error_count;

    werror("%d tests failed.\n%d tests succeeded\n", error_count, success_count);

    return 0;
}
