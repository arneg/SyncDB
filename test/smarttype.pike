inherit "base";

class A {
    inherit SyncDB.MySQL.SmartType;

    Field id = Integer(KEY, AUTOMATIC);
    Field foo = String();
    Field bar = JSON(DEFAULT(([ "boo" : ({ "floo", 1, 2, 3 }) ])));
    Field n = Integer(DEFAULT(23));

    program datum = class {
        inherit SyncDB.MySQL.Datum;

        void set_foo(string s) {
            this->foo = s;
            save();
        }
    };
}

void _test_update() {
    object db1 = SyncDB.MySQL.Database(`sql, "test");

    db1->create_version_table();

    object db2 = SyncDB.MySQL.Database(`sql, "test");

    db1->register_view("table1", A());
    db2->register_view("table1", A());

    object a = db1->get_table("table1")->put(([ "foo" : "bar" ]));
    object a2 = db2->get_table("table1")->fetch_by_id(a->id)[0];

    if (a == a2) error("tables should not be the same.\n");

    a->set_foo("flu");

    if (a2->foo != "flu")
        error("Update did not propagate across databases: %O vs %O\n",
              (mapping)a2, (mapping)a);

    a->drop();

    if (!a2->is_deleted()) {
        error("Delete did not propagate across databases.\n");
    }
}

void _test_insert() {
    object db = SyncDB.MySQL.Database(`sql, "test");

    db->create_version_table();

    object tbl = db->register_view("table1", A());

    array(mapping) a = map(enumerate(1000), sample_data, tbl->schema);

    void writer() {
        array(object) res = tbl->put(a);

        for (int i = 0; i < 300; i++) {
            object o = random(res);
            o->n++;
            o->save();
        }
    };

    int num_reads = 0;

    void reader() {
        while (1) {
            array(object) tmp = tbl->fetch(tbl->id->Equal(random(1000)));
            num_reads++;
        }
    };

    object r = Thread.Thread(reader);
    object w = Thread.Thread(writer);

    w->wait();
    r->kill();

    werror("Managed to do %d reads.\n", num_reads);
}

void _test_parallel_torture() {
    int N = 100;
    object db1 = SyncDB.MySQL.Database(`sql, "test");

    db1->create_version_table();

    object db2 = SyncDB.MySQL.Database(`sql, "test");

    object tbl1 = db1->register_view("table1", A());
    object tbl2 = db2->register_view("table1", A());

    void writer(object tbl, int n) {
        array(object) res = ({ });

        for (int i = 0; i < N; i++) {
            object o = tbl->put(sample_data(tbl->schema, i + n*N));
            res += ({ o });
        }
    };

    array t = ({ });

    for (int n = 0; n < 10; n++) {
        t += ({ Thread.Thread(writer, n & 1 ? tbl1 : tbl2, n) });
    }

    int num_reads = 0;

    void reader(object tbl) {
        while (1) {
            array(object) tmp = tbl->fetch(tbl->id->Equal(random(10 * N)));
            num_reads++;
        }
    };

    array r = ({ Thread.Thread(reader, tbl1), Thread.Thread(reader, tbl2) });

    t->wait();
    r->kill();

    werror("Managed to do %d reads.\n", num_reads);
}
