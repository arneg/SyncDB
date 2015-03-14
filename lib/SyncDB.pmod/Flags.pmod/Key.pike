inherit .Index;
inherit .Unique;

constant is_key = 1;
constant is_not_null = 1;

string encode_json() {
    return "(new SyncDB.Flags.Key())";
}

string sql_type(function(mixed:string) encode_sql) {
    return "PRIMARY KEY";
}

array(SyncDB.MySQL.Query) flag_definitions(object type) {
    return ({ SyncDB.MySQL.Query("PRIMARY KEY") });
}
