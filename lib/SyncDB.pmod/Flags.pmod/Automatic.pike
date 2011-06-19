inherit .Base;

constant is_automatic = 1;

string encode_json() {
    return "(new SyncDB.Flags.Automatic())";
}
