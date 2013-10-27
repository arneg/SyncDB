array(object) tables = set_weak_flag(({}), Pike.WEAK_VALUES);

int interval;

// this might get called more often
void create(int interval) {
    remove_call_out(request_updates);
    this_program::interval = interval;
    if (interval) {
        call_out(request_updates, interval);
    }
}

void register_table(object table) {
    tables = set_weak_flag(tables + ({ table }), Pike.WEAK_VALUS);
}

void unregister_table(object table) {
    tables = set_weak_flag(tables - ({ table }), Pike.WEAK_VALUS);
}

void request_updates() {

    foreach (tables;; object o) {
        if (o) o->request_update();
    }

    if (interval) {
        call_out(request_updates, interval);
    }
}

void destroy() {
    remove_call_out(request_updates);
}

void signal_updates(object table, SyncDB.Version version, array(mapping) rows) {
    foreach (tables;; object o) {
        if (o && o != table) {
            o->handle_update(version, rows);
        }
    }
}
