int main() {
    object sql = Sql.Sql("mysql://root@localhost/interSync");


    object schema =  SyncDB.Schema(
	SyncDB.Types.Integer("id", SyncDB.Flags.Key(),
				    SyncDB.Flags.Automatic(),
				    SyncDB.Flags.Join(([ "two" : "id" ]))),
	SyncDB.Types.String("name"),
	SyncDB.Types.String("email"),
	// two
	SyncDB.Types.Range("date", 
	    SyncDB.Types.Date("startdate", Calendar.Second),
	    SyncDB.Types.Date("stopdate", Calendar.Second)
	),
	SyncDB.Types.String("firstname",
			    SyncDB.Flags.Foreign("two", "firstname")),
	SyncDB.Types.String("lastname",
			    SyncDB.Flags.Foreign("two", "lastname"))
    );
    object table = SyncDB.MySQL.Table("one", sql, schema, "one");
    add_constant("sql", sql);
    add_constant("schema", schema);
    add_constant("table", table);
    void PR(mixed ... args) {
	werror("%O\n", args);
    };
    add_constant("pr", PR);

    
    Tools.Hilfe.StdinHilfe();
}
