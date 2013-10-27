class RowBased(mapping row) {

    mixed `[](mixed key) {
	return row[key];
    }

    mixed `[]=(mixed key, mixed val) {
	return row[key] = val;
    }

    mixed `->=(mixed key, mixed val) {
	return row[key] = val;
    }

    int _mappingp() {
	return 1;
    }
}

class DeletedRow {
    inherit RowBased;
}

mapping(mixed:mapping(string:object)) managers = ([]);

object get_update_manager(mixed db, string table, void|int interval) {
    if (!has_index(managers, db)) {
        managers[db] = set_weak_flag(([]), Pike.WEAK_VALUES);
    }

    mapping m = managers[db];
    object manager = m[table];

    if (!manager) {
        manager = SyncDB.UpdateManager(interval);
        m[table] = manager;
    } else if (interval) {
        int cinterval = manager->interval;
        if (!cinterval || interval < cinterval) {
            manager->create(interval);
        }
    }

    return manager;
}
