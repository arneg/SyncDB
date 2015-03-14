int get_priority() {
    return 50;
}

// hide flag by default
string encode_json() { return ""; }

string sql_type(function(mixed:string) encode_sql) { return ""; }

array(SyncDB.MySQL.Query) flag_definitions(object type) { return ({ }); }
