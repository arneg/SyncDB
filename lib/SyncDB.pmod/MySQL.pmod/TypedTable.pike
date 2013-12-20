inherit SyncDB.MySQL.Table;
mapping cache = set_weak_flag(([]), Pike.WEAK_VALUES);
Thread.Mutex mutex = Thread.Mutex();
function|program prog;

void create(string dbname, function(void:Sql.Sql) cb, SyncDB.Schema schema, string table,
            void|function|program prog) {
    
    this_program::prog = prog||.Datum;
    ::create(dbname, cb, schema, table);
}

void low_select(mixed ... args) {
    ::select(@args);
}

void select(object filter, function(int, mixed...:void) cb, mixed ... extra) {
    select_complex(filter, 0, 0, cb, extra);
}

void select_complex(object filter, object order, object limit,
                    function(int, mixed...:void) cb, mixed ... extra) {
    void _cb(int err, mixed v) {
        if (err) {
            cb(err, v, @extra);
            return;
        }
        object key = mutex->lock();

        foreach(v;int i; mapping|object row) if (mappingp(row)) {
            mixed id = get_unique_identifier(row);
            object o;

            if (!objectp(o = cache[id])) {
                o = prog();
                o->init(row, this);
                cache[id] = o;
            } else {
                o->update(row);
            }

            v[i] = o;

        }

        cb(0, v, @extra);
    };
    if (object_program(filter) == SyncDB.MySQL.Filter.Equal) {
        object key = mutex->lock();
        function quote = sql->quote;
        string id = filter->encode_sql(this, quote)->render(quote);
        if (has_index(cache, id)) {
            object o = cache[id];
            destruct(key);
            cb(0, ({ o }));
            return;
        }
        destruct(key);
    }
    ::select_complex(filter, order, limit, _cb);
}

void insert(object|mapping row, function cb, mixed ... extra) {
    void _cb(int err, mixed v) {
        if (err) {
            cb(err, v, @extra);
        } else {
            object key = mutex->lock();
            string id = get_unique_identifier(v);
            //mixed id = schema->get_unique_identifier(v);
            if (cache[id]) {
                cache[id]->update(v);
            } else {
                cache[id] = prog();
                cache[id]->init(v, this);
            }
            cb(err, cache[id], @extra);
        }
    };
    if (objectp(row)) {
        row = (mapping)row;
    }
    ::insert(row, _cb);
}

array(object) fetch(void|object filter, void|object order, void|object limit) {
    mixed ret;
    void cb(int err, mixed v) {
        if (!err) ret = v;
        else throw(v);
    };
    select_complex(filter||SyncDB.MySQL.Filter.TRUE, order, limit, cb);
    return ret;
}

object put(mapping row) {
    object ret;
    void cb(int err, mixed v) {
        if (!err) ret = v;
        else throw(v);
    };
    insert(row, cb);
    return ret;
}

mixed `->(string index) {
    if (has_prefix(index, "fetch_by_")) {
        string field = index[sizeof("fetch_by_")..];
        array(object) cb(mixed v) {
            return fetch(schema[field]->Equal(v));
        };
        return cb;
    }

    return call_function(::`->, index, this);
}

#if constant(Roxen)
mapping(mixed:array) _requests = ([]);
array(mixed) _table_requests = ({ });

void register_request(mixed cachekey, void|mixed id) {
    if (id) {
        if (!has_index(_requests, id)) {
            _requests[id] = ({ });
        }
        _requests[id] += ({ cachekey });
    } else {
        id = cachekey;
        id->misc->cachekey->add_activation_cb(low_register_cachekey);
    }
}

void low_register_cachekey(mixed cachekey) {
    _table_requests += ({ cachekey });
}

void invalidate_requests(mixed id) {
    array keys = _table_requests;
    
    if (has_index(_requests, id)) {
        keys += m_delete(_requests, id);
    }

    if (!keys || !sizeof(keys)) return;

    keys = filter(keys, keys);

    if (sizeof(keys)) {

        // Tristate cached serve stale cache entries once. we cannot
        // tolerate that, so we have to go for a manual destruct here.
        //map(keys, roxen.invalidate);
        map(keys, destruct);
        _table_requests = ({});
    }
}

void signal_update(SyncDB.Version version, void|array(mapping) rows) {
    ::signal_update(version, rows);

    foreach (rows;; mapping row) {
        mixed id = get_unique_identifier(row);
        invalidate_requests(id);
    }
}

void destroy() {
    map(indices(_requests), invalidate_requests);
}
#endif

void handle_update(SyncDB.Version version, void|array(mapping) rows) {
    ::handle_update(version, rows);

    foreach (rows;; mapping row) {
        mixed id = get_unique_identifier(row);
#if constant(Roxen)
        invalidate_requests(id);
#endif

        if (cache[id]) {
            // we make sure to copy it here, since complex types could be shared otherwise
            cache[id]->update(copy_value(row));
        }
    }
}
