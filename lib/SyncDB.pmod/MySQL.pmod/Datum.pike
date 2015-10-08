#if constant(Roxen)
void register_request(object id) {
    table->register_request(id);
}

void invalidate_requests() {
    table->invalidate_requests();
}
#endif
final protected Thread.Mutex mutex = Thread.Mutex();

Thread.MutexKey lock() {
    return mutex->lock();
}

protected mapping _modified = ([]);
protected mapping _data = ([]);
object table;

array `fields() {
    return table->fields;
}

mapping `nfields() {
    return table->nfields;
}

object `smart_type() {
    return table->smart_type;
}

int(0..1) inherits(program p) {
    return Program.inherits(object_program(smart_type), p);
}

int(0..1) is_program(program p) {
    return p == object_program(smart_type);
}

int(0..1) is_dummy() {
    return !this->id;
}

void check_value(string name, mixed key) { }
void onchange() { }

mixed unique_identifier() {
    return smart_type->unique_identifier(this);
}

void init(mapping _data, object table) {
    this_program::_data += _data;
    this_program::table = table;
}

protected mixed `[](string name) {
    if (has_index(_data, name)) return _modified[name] || _data[name];
    return UNDEFINED;
}

mixed `[]=(string name, mixed value) {
    if (!has_index(_data, name)) error("No such index %O\n", name);

    check_value(name, value);
    _modified[name] = value;
    return value;
}

void set_dirty(string name) {
    _modified[name] = _data[name];
}

protected void generic_cb(int err, mixed v) {
    if (err) {
        werror("%O->save() failed\n", this);
        master()->handle_error(v);
    }
}

void update(mapping _data) {
    if (_data->version != this_program::_data->version) {
        this_program::_data = _data;
        call_out(onchange, 0);
    }
}

void apply_changes(mapping changes) {
    _data += changes;
}

string describe() {
    // this will always be there
    return (string)this->id;
}

void force_update() {
    object f = my_filter();
    mapping ret;

    mixed err = catch {
        ret = table->low_select_complex(f)[0];
    };
    if (err) {
        werror("we really cannot recover from this! db seems to be broken:\n");
        throw(err);
    }

    update(ret);
}

void modify(mapping diff) {
    if (is_deleted()) error("Modifying deleted record.\n");
    foreach (diff; string name; mixed value) check_value(name, value);
    _modified += diff - ({ "version" });
}

protected mapping id_data() {
    mapping m = ([]);
    string key = table->schema->key;
    m[key] = _data[key];
    return m;
}

protected object my_versioned_filter() {
    object schema = table->schema;
    object id_type = schema->id;

    mixed id = _data[id_type->name];
    int old_version = _data->version;

    return id_type->Equal(id) & schema->version->Equal(old_version);
}

protected object my_filter() {
    object id_type = table->schema->id;

    mixed id = _data[id_type->name];

    return id_type->Equal(id);
}

void save_unlocked() {
    if (save_id) {
        remove_call_out(save_id);
        save_id = 0;
    }
    if (is_deleted()) error("Modifying deleted record.\n");

    int affected;

    object schema = table->schema;
    object id_type = schema->id;

    mixed id = _data[id_type->name];
    int old_version = _data->version;

    object filter = id_type->Equal(id) &
                    schema["version"]->Equal(old_version);

    mixed err = catch {
        affected = table->update(_modified, filter, id);
    };

    if (err) {
        master()->handle_error(err);
    }

    if (!affected) {
        force_update();
        throw(SyncDB.Error.Collision(table, _data->version, old_version));
    }

    _data += _modified;
    _data->version = old_version + 1;
    _modified = ([]);

    return;
}

protected mixed save_id;

int(0..1) is_deleted() {
    return !has_index(_data, "version");
}

void mark_deleted() {
    m_delete(_data, "version");
    _modified = ([]);

    if (save_id) {
        remove_call_out(save_id);
        save_id = 0;
    }
}

void drop_throw() {
    if (is_deleted()) return;
    if (save_id) {
        remove_call_out(save_id);
        save_id = 0;
    }
    table->drop(my_filter());
}

int(0..1) drop() {
    return !catch(drop_throw());
}

void save() {
    object key = mutex->lock();
    if (sizeof(_modified))
        save_unlocked();
}

void save_later(void|int s) {
    object key = mutex->lock();
#if constant(Roxen)
    invalidate_requests();
#endif
    if (save_id) return;
    save_id = call_out(save, s||5);
}

string _sprintf(int type) {
    return sprintf("%O(%O, %d modified)", this_program, describe(), sizeof(_modified));
}

mixed cast(string type) {
    if (type == "mapping") {
        return _data + _modified;
    }
    error("Cannot cast %O to %s\n", this, type);
}

void destroy(int reason) {
    if (save_id) {
        if (reason == Object.DESTRUCT_GC) {
            werror("save_id != 0 in %O->destroy(%d).\n", this, reason);
            if (find_call_out(save_id)) {
                werror("Callout still active, refcounting bug?\n");
            }
        }
        remove_call_out(save_id);
    }
}

mapping clone() {
    object schema = table->schema;
    return (mapping)this - filter(schema->fields, schema->fields->is->unique)->name;
}

object get_remote_table(string name, void|program type) {
    return table->update_manager && table->update_manager->get_table(name, type);
}

object remote_table(string name, void|program type) {
    if (name == table->table_name() && (!type || type == object_program(table->smart_type))) return table;
    return table->remote_table(name, type);
}
