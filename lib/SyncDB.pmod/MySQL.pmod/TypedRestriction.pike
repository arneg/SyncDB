inherit SyncDB.MySQL.Restriction;

array(object) fetch(void|object filter, void|object order, void|object limit) {
    return table->fetch(filter ? filter & restriction : restriction, order, limit);
}

object put(mapping row, mixed ... args) {
    row += ([]);
    restriction->insert(row);
    return table->put(row, @args);
}

void register_request(mixed cachekey, void|mixed id) {
    table->register_request(cachekey, id);
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

array(object) get_fields() {
    return table->get_fields();
}

mapping(string:object) get_nfields() {
    return table->get_nfields();
}
