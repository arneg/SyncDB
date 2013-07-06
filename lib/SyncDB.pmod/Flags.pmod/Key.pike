inherit .Index;
inherit .Unique;

constant is_key = 1;

string encode_json() {
    return "(new SyncDB.Flags.Key())";
}

string sql_type(function(mixed:string) encode_sql) {
    return "PRIMARY KEY";
}
