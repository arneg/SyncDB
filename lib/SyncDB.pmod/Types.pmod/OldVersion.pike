inherit .Integer;

void create(string name) {
    ::create(name, SyncDB.Flags.Index(), SyncDB.Flags.Unique());
}

string type_name() {
    return "version";
}
