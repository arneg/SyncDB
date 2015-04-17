inherit .Base;

int is_default = 1;

mixed default_value;

void create(mixed v) {
    default_value = v;
}

string encode_json() {
    return "(new SyncDB.Flags.Default())";
}

string sql_type(function(mixed:string) encode) {
    return sprintf("NOT NULL");
}

array(SyncDB.MySQL.Query) flag_definitions(object type) {
    return ({ SyncDB.MySQL.Query("NOT NULL") });
}

string _sprintf(int t) {
    return sprintf("%O(%O)", this_program, default_value);
}
