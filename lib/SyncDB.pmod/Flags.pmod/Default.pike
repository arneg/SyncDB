inherit .Base;

mixed val;

void create(mixed v) {
    val = v;
}

string encode_json() {
    return "(new SyncDB.Flags.Default())";
}

string sql_type(function(mixed:string) encode) {
    return sprintf("NOT NULL DEFAULT %s", encode(val));
}
