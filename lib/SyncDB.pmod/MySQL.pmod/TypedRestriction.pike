inherit SyncDB.MySQL.Restriction;

array(object) fetch(void|object filter, void|object order, void|object limit) {
    return table->fetch(filter ? filter & restriction : restriction, order, limit);
}

object put(mapping row) {
    row += ([]);
    restriction->insert(row);
    return table->put(row);
}

void register_request(mixed cachekey, void|mixed id) {
    table->register_request(cachekey, id);
}

void invalidate_requests(mixed id) {
    table->invalidate_requests(id);
}