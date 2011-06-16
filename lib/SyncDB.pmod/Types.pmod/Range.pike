inherit .Vector;

void create(array(string) fields, SyncDB.Flags.Base ... flags) {
    ::create(.Date, fields, @flags);
}
