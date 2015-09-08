inherit .Integer;

void create(string name) {
    ::create(name, SyncDB.Flags.Default(1));
}

object previous_type() {
    return .OldVersion(name);
}

string type_name() {
    return "version";
}
