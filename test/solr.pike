inherit "base";

class A {
    inherit SyncDB.MySQL.SmartType;

    Field id = Integer(KEY, AUTOMATIC);
    Field foo = String(SOLR_FIELD(([ "name" : "data_*", "type" : "syncdb_string" ])), SOLR_SETTINGS(([ "stored" : Val.true ])));
    Field bar = String();
    Field data = JSON(SOLR_FIELD(([ "name" : "data_*", "type" : "syncdb_string" ])), DEFAULT(([ "boo" : ({ "floo", 1, 2, 3 }) ])));
    Field n = Integer(DEFAULT(23), SOLR_SETTINGS(0));

    program datum = class {
        inherit SyncDB.MySQL.Datum;

        void set_foo(string s) {
            this->foo = s;
            save();
        }
    };
}
constant arg_options = ({
    ({ "host", Getopt.HAS_ARG, "-s" }),
    ({ "port", Getopt.HAS_ARG, "-p" }),
    ({ "core", Getopt.HAS_ARG, "-c" }),
    ({ "db", Getopt.HAS_ARG, "-d" }),
});

int main(int argc, array(string) argv) {
    array tmp = Getopt.find_all_options(argv, arg_options, 1);
    mapping options = mkmapping(tmp[*][0], tmp[*][1]);

    options->port = (int)options->port;

    argv = Getopt.get_args(argv, 1)[1..];

    sql_path = options->db;

    SolR.Instance solr = SolR.Instance(options->host, options->port);
    SolR.Collection core = solr->collection(options->core);

    catch (sql->query("DROP DATABASE `solr_test`"));

    sql->query("CREATE DATABASE `solr_test`");

    sql_path += "solr_test";

    object db = SyncDB.MySQL.Database(`sql, "test");
    db->create_version_table();
    object smart_type = A();
    object tbl = db->register_view("table1", smart_type);

    object index = SyncDB.SolR.TableIndex(tbl, core);

    return -1;
}
