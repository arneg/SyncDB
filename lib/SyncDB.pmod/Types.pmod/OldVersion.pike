inherit .Integer;

void create(string name) {
    ::create(name, SyncDB.Flags.Index(), /*SyncDB.Flags.Unique(),*/ SyncDB.Flags.Mandatory(), SyncDB.Flags.Default(0));
}

string type_name() {
    return "version";
}
