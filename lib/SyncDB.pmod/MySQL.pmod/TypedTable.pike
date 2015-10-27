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
    register_trigger("after_update", after_update);
    register_trigger("after_delete", after_delete);
}

array(object) `fields() {
    return smart_type->fields;
}

mapping(string:object) `nfields() {
    return smart_type->nfields;
}

void set_database(void|object o) {
    if (database) smart_type->remove_dependencies(this, database);

    ::set_database(o);

    if (database) smart_type->create_dependencies(this, database);
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
    array(object) rows;
    
    mixed err = catch(rows = fetch(filter, order, limit));

    if (err) {
        cb(1, err, @extra);
    } else {
        cb(0, rows, @extra);
    }
}

void insert(object|mapping row, function cb, mixed ... extra) {
    object o;
    
    mixed err = catch(o = put(row));

    if (err) {
        cb(1, err, @extra);
    } else {
        cb(0, o, @extra);
    }
}

array(object) fetch(void|object f, void|object order, void|object limit) {
    if (f && !order && !limit) {
        array values = f->get_all_field_values(schema->key);

        if (values) {
            values = map(values, cache);
            
            if (Array.all(values, objectp)) {
                array hits = map(values, f->test);

                int tmp = min(@hits);

                // they all match
                if (tmp == 1) return values;

                // some don't, but we know for sure
                if (tmp == 0) {
                    return filter(values, hits);
                }

                // we don't know, some might not match
            }
        }
    }

    array rows = low_select_complex(f, order, limit);
    object key = mutex->lock();

    foreach(rows;int i; mapping|object row) if (mappingp(row)) {
        mixed id = get_unique_identifier(row);
        object o;

        if (!objectp(o = cache[id])) {
            o = prog();
            o->init(row, this);
            cache[id] = o;
        } else {
            o->update(row);
        }

        rows[i] = o;
    }

    return rows;
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
    return low_count_rows(filter);
}

mixed `->(string index) {
    if (has_prefix(index, "fetch_by_")) {
        string field = index[sizeof("fetch_by_")..];
        array(object) cb(mixed v, void|object order, void|object limit) {
            if (arrayp(v)) {
                return fetch(schema[field]->In(v), order, limit);
            } else {
                if (field == schema->key) {
                    if (has_index(cache, v)) return ({ cache[v] });
                }
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
mapping(mixed:int) _table_requests = set_weak_flag(([]), Pike.WEAK_INDICES);

void register_request(object id) {
    id->misc->cachekey->add_activation_cb(low_register_cachekey);
}

void low_register_cachekey(mixed cachekey) {
    _table_requests[cachekey] = 1;
}

void invalidate_requests(void|mixed id) {
    if (!sizeof(_table_requests)) return;

    array keys = indices(_table_requests);

    _table_requests = set_weak_flag(([]), Pike.WEAK_INDICES);

#ifdef CACHE_TRACE
    werror("invalidating %d\n", sizeof(keys));
#endif
    // Tristate cached serve stale cache entries once. we cannot
    // tolerate that, so we have to go for a manual destruct here.
    //map(keys, roxen.invalidate);
    map(keys, destruct);
}

void after_change(object table) {
    ::after_change(table);
    invalidate_requests();
}
#endif

void destroy() {
    map(values(cache), destruct);
}

void after_delete(object table, mapping keys) {
    mixed id = get_unique_identifier(keys);

    object datum = m_delete(cache, id);
    if (datum) datum->mark_deleted();
}

void after_update(object table, object filter, mapping changes, void|mixed id) {
    if (table == this) return;
    if (id) {
        object datum = cache[id];
        if (datum) {
            datum->apply_changes(changes);
        }
    } else {
        fetch(filter);
    }
}

string _sprintf(int type) {
    return sprintf("%O(%O, %O)", this_program, table_name(), smart_type);
}
