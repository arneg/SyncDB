inherit "base";

class A {
    inherit SyncDB.MySQL.SmartType;

    Field id = Integer(KEY, AUTOMATIC);
    Field foo = String();

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
