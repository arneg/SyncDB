inherit .Vector;

void create(string from, string to, SyncDB.Flags.Base ... flags) {
    ::create(.Date, ({ from, to }), @flags);
}
