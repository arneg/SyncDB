inherit .Base;

constant is_mandatory = 1;

string encode_json() {
    return "(new SyncDB.Flags.Mandatory())";
}
