array(SyncDB.Flags.Base) flags;
int(0..1) is_index;
int(0..1) is_unique;
int(0..1) is_hidden;
int priority = 50;

void create(SyncDB.Flags.Base ... flags) {
    this_program::flags = flags;

    foreach (flags;; SyncDB.Flags.Base flag) {
	is_hidden = max(is_hidden, flag->is_hidden);
	is_unique = max(is_unique, flag->is_unique);
	is_index = max(is_index, flag->is_index);
    }
}

int(0..1) `is_key() {
    return is_index && is_unique;
}

string decode_sql(string s) {
    return s;
}
