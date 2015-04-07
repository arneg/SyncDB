inherit SyncDB.MySQL.Table;
mapping cache = set_weak_flag(([]), Pike.WEAK_VALUES);
Thread.Mutex mutex = Thread.Mutex();
function|program|object prog;
object smart_type;

void create(string dbname, function(void:Sql.Sql) cb, SyncDB.Schema schema, string table,
            object smart_type) {
    this_program::smart_type = smart_type;
    this_program::prog = smart_type->get_datum();
    ::create(dbname, cb, schema, table);
}

array(object) `fields() {
    return smart_type->fields;
}

mapping(string:object) `nfields() {
    return smart_type->nfields;
}

void set_database(void|object o) {
    array(object) fields = this_program::fields;

    if (database) {
        foreach (fields;; object field) {
            if (field->remove_dependencies)
                field->remove_dependencies(this, database);
        }
    }

    ::set_database(o);

    if (o) {
        foreach (fields;; object field) {
            if (field->create_dependencies)
                field->create_dependencies(this, o);
        }
    }
}

void low_select(mixed ... args) {
    ::select(@args);
}

void select(object filter, function(int, mixed...:void) cb, mixed ... extra) {
    select_complex(filter, 0, 0, cb, extra);
}

object dummy(void|mapping data) {
    object o = prog();
    o->init(data||([]), this);
    return o;
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
    if (object_program(filter) == SyncDB.MySQL.Filter.Equal && filter->type->is->key) {
        object key = mutex->lock();
        mixed id = filter->value;

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
            object o;
            //mixed id = schema->get_unique_identifier(v);
            if (o = cache[id]) {
                o->update(v);
            } else {
                cache[id] = o = prog();
                o->init(v, this);
            }
            call_out(cache[id]->onchange, 0);
            cb(err, o, @extra);
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
        else {
            werror("fetch failed:\n");
            master()->handle_error(v);
        }
    };
    select_complex(filter||SyncDB.MySQL.Filter.TRUE, order, limit, cb);
    return ret;
}

array(object)|object put(array(mapping)|mapping row) {
    object filter = low_insert(arrayp(row) ? row : ({ row }));
    array(object) ret = fetch(filter);
    if (!sizeof(ret)) error("Could not fetch row after inserting %O\nfilter: %O\n", row, filter);
    return arrayp(row) ? ret : ret[0];
}

void just_put(array(mapping)|mapping row) {
    low_insert(arrayp(row) ? row : ({ row }));
}

int(0..) count(void|object filter) {
    int(0..) ret;
    void cb(int err, mixed v) {
        if (!err) ret = v;
        else {
            werror("count failed:\n");
            master()->handle_error(v);
        }
    };
    count_rows(filter||SyncDB.MySQL.Filter.TRUE, cb);
    return ret;
}

mixed `->(string index) {
    if (has_prefix(index, "fetch_by_")) {
        string field = index[sizeof("fetch_by_")..];
        array(object) cb(mixed v, void|object order, void|object limit) {
            if (arrayp(v)) {
                return fetch(schema[field]->In(v), order, limit);
            } else {
                return fetch(schema[field]->Equal(v), order, limit);
            }
        };
        return cb;
    }

    return call_function(::`->, index, this);
}

object restrict(object filter) {
    return .TypedRestriction(this, filter);
}

#if constant(Roxen)
mapping(mixed:array) _requests = ([]);
array(mixed) _table_requests = ({ });

void register_request(mixed ... args) {
    if (sizeof(args) == 2) {
        object cachekey;
        mixed id;
        cachekey = args[0];
        id = args[1];
        if (!has_index(_requests, id)) {
            _requests[id] = ({ });
        }
        _requests[id] += ({ cachekey });
    } else {
        object id;
        id = args[0];
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

#ifdef CACHE_TRACE
        werror("invalidating %O\n", keys);
#endif
        // Tristate cached serve stale cache entries once. we cannot
        // tolerate that, so we have to go for a manual destruct here.
        //map(keys, roxen.invalidate);
        map(keys, destruct);
        _table_requests = ({});
    }
}

void signal_update(SyncDB.Version version, void|array(mapping) rows) {
    ::signal_update(version, rows);

    trigger("change");

    int(0..1) is_delete = version->is_deleted();

    foreach (rows;; mapping row) {
        mixed id = get_unique_identifier(row);
        invalidate_requests(id);
        // this is the local trigger, we dont need to notify the data
        // themselves, since they were the source of the change event.

        if (is_delete && cache[id]) {
            m_delete(cache, id);
        }
    }
}

void destroy() {

    map(indices(_requests), invalidate_requests);

    ::destroy();
}
#endif

void handle_update(SyncDB.Version version, void|array(mapping) rows) {
    ::handle_update(version, rows);

    int(0..1) is_delete = version->is_deleted();

    trigger("change");

    foreach (rows;; mapping row) {
        mixed id = get_unique_identifier(row);
#if constant(Roxen)
        invalidate_requests(id);
#endif

        if (cache[id]) {
            // we make sure to copy it here, since complex types could be shared otherwise
            // ->update will call onchange!
            if (is_delete) {
                m_delete(cache, id)->mark_deleted();
            } else {
                cache[id]->update(copy_value(row));
            }
        }
    }
}
