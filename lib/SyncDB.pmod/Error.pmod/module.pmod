class Collision {
    inherit Error.Generic;

    SyncDB.Version actual, expected;
    SyncDB.Table table;

    void create(SyncDB.Table table, SyncDB.Version actual, SyncDB.Version expected, void|array backtrace) {
        this_program::table = table;
        this_program::actual = actual;
        this_program::expected = expected;

        ::create(sprintf("Collision: expected version %O, found %O\n", expected, actual), backtrace);
    }
}

class Denied { }
