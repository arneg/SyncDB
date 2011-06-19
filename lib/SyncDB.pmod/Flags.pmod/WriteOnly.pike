inherit .Base;

constant is_readable = 0;

string encode_json() {
    return "(new SyncDB.Types.WriteOnly())";
}
