inherit .Mandatory;

constant is_unique = 1;

string encode_json() {
    return "(new SyncDB.Types.Unique())";
}

string sql_type(function(mixed:string) encode) {
    return "UNIQUE";
}
