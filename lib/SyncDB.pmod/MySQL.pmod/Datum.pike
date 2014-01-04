inherit .SmartType;

mapping _modified = ([]);

array requests = ({ });

#if constant(Roxen)
void register_request(object id) {
    id->misc->cachekey->add_activation_cb(table->register_request, unique_identifier());
}
#endif

private mapping _data = ([]);
object table;

void check_value(string name, mixed key) { }

mixed unique_identifier() {
    return table->get_unique_identifier(_data);
}

void init(mapping _data, object table) {
    this_program::_data = _data;
    this_program::table = table;
}

mixed `->(string name) {
    if (has_index(_modified, name)) return _modified[name];
    if (has_index(_data, name)) return _data[name];
    return call_function(::`->, name, this);
}

mixed `[](string name) {
    if (has_index(_modified, name)) return _modified[name];
    if (has_index(_data, name)) return _data[name];
    return UNDEFINED;
}

mixed `->=(string name, mixed value) {
    if (has_index(_data, name)) {
        check_value(name, value);
        _modified[name] = value;
        return value;
    }

    return call_function(::`->=, name, value, this);
}

void generic_cb(int err, mixed v) {
    if (err) {
        werror("%O->save() failed: %O\n", this, v);
    }
}

void update(mapping _data) {
    if (_data->version != this_program::_data->version) {
        this_program::_data = _data;
        call_out(onchange, 0);
    }
}

void onchange() { }

string describe() {
    // this will always be there
    return (string)this->id;
}

protected void force_update() {
    object tid = table->schema->id;
    object f = tid->Equal(_data[tid->name]);
    mapping ret;

    table->low_select(f, lambda (int err, mixed v) {
        if (err) {
            error("we really cannot recover from this! db seems to be broken\n");
        } else {
            ret = v[0];
        }
    });
    update(ret);
}

void modify(mapping diff) {
    foreach (diff; string name; mixed value) check_value(name, value);
    _modified += diff - ({ "version" });
}

Thread.Mutex mutex = Thread.Mutex();

protected mapping id_data() {
    mapping m = ([]);
    string key = table->schema->key;
    m[key] = _data[key];
    return m;
}

protected void save_unlocked(function(int, mixed...:void)|void cb, mixed ... extra) {
    if (!cb) cb = generic_cb;
    if (!sizeof(_modified)) {
        cb(0);
        return;
    }

    void _cb(int err, mixed v) {
        if (err) {
            if (object_program(err) == SyncDB.Error.Collision) {
                // this is a syncdb collision. lets update _data and see from there
                werror("WARNING: recovering from SyncDB collision. Data might get overwritten.\n");
                force_update();
            }
            cb(1, v, @extra);
        } else {
            // this is an explicit update, and it should bypass the update() method which is supposed
            // to be overloaded to handle change events.
            call_out(onchange, 0);
            _data = v;
            _modified = ([]);
            //werror("updated %O to %O\n", this, _data);
            cb(0, 0, @extra);
        }
    };

    table->update(_modified + id_data(), _data->version, _cb);
}

int(0..1) drop() {
    int(0..1) ret;
    void cb(int(0..1) err, mixed b) {
        ret = err;
    };
    delete(cb);
    if (!ret) {
        save_id && remove_call_out(save_id);
        return 1;
    } else
        return 0;
}

void delete(function(int, mixed...:void)|void cb, mixed ... extra) {
    table->delete(id_data(), _data->version, cb||generic_cb);
}

void save(function(int, mixed...:void)|void cb, mixed ... extra) {
    object key = mutex->lock();
    if (save_id) {
        remove_call_out(save_id);
        save_id = 0;
    }
    save_unlocked(cb, @extra);
}

mixed save_id;

void save_later(void|int s) {
    object key = mutex->lock();
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

void destroy() {
    // save();
}

object get_table(function(void:Sql.Sql) get_sql, string name, void|function|program prog) {
    return .TypedTable(name, get_sql, schema, name, prog||this_program);
}

void create_table(function(void:Sql.Sql) get_sql, string name) {
    .create_table(get_sql(), name, schema);
}
