inherit .Base;

constant is_index = 1;

string encode_json() {
    return "(new SyncDB.Flags.Index())";
}
