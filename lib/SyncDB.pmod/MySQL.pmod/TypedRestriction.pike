inherit SyncDB.MySQL.Restriction;

array(object) fetch(void|object filter, void|object order, void|object limit) {
    return table->fetch(filter & restriction, order, limit);
}

int(0..) update(mapping changes, object filter, mixed ... extra) {
    return table->update(changes, filter & restriction, @extra);
}

object put(array(mapping)|mapping row) {
    if (arrayp(row)) {
        mapping tmp = ([]);
        restriction->insert(tmp);
        row = map(row, predef::`+, tmp);
    } else {
        row += ([]);
        restriction->insert(row);
    }
    return table->put(row);
}

void just_put(array(mapping)|mapping row) {
    if (arrayp(row)) {
        mapping tmp = ([]);
        restriction->insert(tmp);
        row = map(row, predef::`+, tmp);
    } else {
        row += ([]);
        restriction->insert(row);
    }
    table->just_put(row);
}

int(0..) count(void|object filter) {
    return table->count(filter & restriction);
}

void register_request(mixed ... args) {
    table->register_request(@args);
}

void invalidate_requests(mixed id) {
    table->invalidate_requests(id);
}

mixed `->(string name) {
    if (has_prefix(name, "fetch_by_")) {
        string field = name[sizeof("fetch_by_")..];
        array(object) cb(mixed v, void|object order, void|object limit) {
            if (arrayp(v)) {
                return fetch(table->schema[field]->In(v), order, limit);
            } else {
                return fetch(table->schema[field]->Equal(v), order, limit);
            }
        };
        return cb;
    }

    return ::`->(name);
}

object dummy(void|mapping m) {
    object o = table->prog();
    if (!m) m = ([]);
    restriction->insert(m);
    o->init(m, table);
    return o;
}
