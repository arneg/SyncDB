inherit "base";

class A {
    inherit SyncDB.MySQL.SmartType;

    Field id = Integer(KEY, AUTOMATIC);
    Field foo = String();


    class FOO {
        inherit SyncDB.MySQL.Datum;

        void set_foo(string s) {
            this->foo = s;
            save();
        }
    };

    protected program datum = FOO;
}

object table;

int main(int argc, array(string) argv) {
    sql_path = argv[-1]; 
    A()->create_table(`sql, "smarttype");
    table = A()->get_table(`sql, "smarttype");

    object a = table->put(([
        "foo" : "test",
    ]));

    a->set_foo("bar");

    return 0;
}
