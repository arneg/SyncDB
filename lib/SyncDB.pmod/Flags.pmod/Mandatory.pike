inherit .Base;

constant is_mandatory = 1;

string encode_json() {
    return "(new SyncDB.Flags.Mandatory())";
}

string sql_type() {
    return "NOT NULL";
}
