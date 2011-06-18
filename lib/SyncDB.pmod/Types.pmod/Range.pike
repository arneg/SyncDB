inherit .Vector;

void create(string name, string from, string to, SyncDB.Flags.Base ... flags) {
    ::create(name, .Date, ({ from, to }), @flags);
}
