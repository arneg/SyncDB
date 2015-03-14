inherit .Base;

constant is_mandatory = 1;

string encode_json() {
    return "(new SyncDB.Flags.Mandatory())";
}

string sql_type(function(mixed:string) encode) {
    return "NOT NULL";
}

array(SyncDB.MySQL.Query) flag_definitions(object type) {
    return ({ SyncDB.MySQL.Query("NOT NULL") });
}
