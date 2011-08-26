inherit "roxen-module://dbtable";

constant module_name = "SyncDB: Test table 1";

void create() {
    schema = SyncDB.Schema(
        SyncDB.Types.Integer("id", SyncDB.Flags.Key(),
                                    SyncDB.Flags.Automatic(),
                                    SyncDB.Flags.Join(([ "two" : "id" ]))),
        SyncDB.Types.String("name"),
        SyncDB.Types.String("email"),
        SyncDB.Types.Range("date",
            SyncDB.Types.Date("startdate", Calendar.Second),
            SyncDB.Types.Date("stopdate", Calendar.Second),
            SyncDB.Flags.Index()
        ),
        // two
        SyncDB.Types.String("firstname", SyncDB.Flags.Foreign("two", "firstname")),
        SyncDB.Types.String("lastname", SyncDB.Flags.Foreign("two", "lastname")),
    );
    ::create();
}
