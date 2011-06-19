inherit .Base;

constant is_writable = 0;

string encode_json() {
    return "(new SyncDB.Flags.ReadOnly())";
}
