int main() {
    object sql = Sql.Sql("mysql://root@localhost/interSync");


    object schema =  SyncDB.Schema(([
	"id" : SyncDB.Types.Integer(SyncDB.Flags.Key(),
				    SyncDB.Flags.Automatic(),
				    SyncDB.Flags.Join(([ "two" : "id" ]))),
	"name" : SyncDB.Types.String(),
	"email" : SyncDB.Types.String(),
	// two
	"firstname" : SyncDB.Types.String(SyncDB.Flags.Foreign("two", "firstname")),
	"lastname" : SyncDB.Types.String(SyncDB.Flags.Foreign("two", "lastname")),
    ]));
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
