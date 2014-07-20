inherit .Base;

constant is_automatic = 1;
constant is_not_null = 1;

string encode_json() {
    return "(new SyncDB.Flags.Automatic())";
}

string sql_type(function(mixed:string) encode) {
    return "AUTO_INCREMENT";
}
