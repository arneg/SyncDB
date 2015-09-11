inherit .Integer;

void create(string name) {
    ::create(name, SyncDB.Flags.Default(1), SyncDB.Flags.Index());
}

object previous_type() {
    return .OldVersion(name);
}

string type_name() {
    return "version";
}

program get_migration_program() {
    return master()->resolv("SyncDB.Migration.Base");
}
