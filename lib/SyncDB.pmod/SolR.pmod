#require constant(SolR)

class EventAggregator(function cb, mixed ... extra) {
    protected private int cbs = 0;
    protected int called = 0;

    protected array results = ({ });

    protected void foo(int n, mixed ... args) {
        results[n] = args;
        called ++;

        if (called != cbs) return;

        args = results;
        results = 0;

        function callback = cb;
        cb = 0;

        callback(@extra, @args);
    }

    function get_cb() {
        results += ({ 0 }); 

        return Function.curry(foo)(cbs++);
    }

    string _sprintf(int t) {
        return sprintf("%O(%O)", this_program, cb);
    }

    void destroy() {
        if (results && cbs) {
            call_out(cb, 0, @extra, @results);
        }
    }

    int _sizeof() {
        return cbs;
    }
}

typedef function(int, mapping , mixed ...:void) select_cb;

class TableIndex {
    mixed err;

    .MySQL.TypedTable table;
    SolR.Collection core;

    void create(.MySQL.TypedTable table, SolR.Collection core) {
        this_program::table = table;
        this_program::core = core;
    }

    private void _select_cb(int ok, mixed result, select_cb cb, mixed ... extra) {
        if (ok) {
            array docs = result->response->docs;

            docs = table->fetch_by_id((array(int))docs->id);

            result->response->docs = docs;
        }

        cb(ok, result, @extra);
    }

    void select(string|mapping query, select_cb cb, mixed ... extra) {
        core->select(query, _select_cb, cb, @extra);
    }
}

class SyncTableIndex {
    inherit TableIndex;

    private int(0..1) needs_resync = 0;

    private void schema_cb(int ok, mixed response) {
        if (ok) {
            mapping my_schema = table->schema->get_solr_schema();
            mapping schema = response->schema;

            mapping my_types = mkmapping(my_schema->fieldTypes->name, my_schema->fieldTypes);
            mapping types = mkmapping(schema->fieldTypes->name, schema->fieldTypes);

            object e = EventAggregator(schema_update_done, my_schema, schema);

            foreach (my_types - indices(types);; mapping type) {
                core->schema->add_field_type(type, e->get_cb(), type);
            }

            /*
            foreach (types - indices(my_types);; mapping type) {
                werror("Need to remove: %O\n", type);
            }
            */

            foreach (my_types & indices(types); string name; mapping type) {
                if (!equal(type, types[name])) {
                    core->schema->replace_field_type(type, e->get_cb(), type);
                }
            }

            if (!sizeof(e)) update_fields(my_schema, schema);
            else needs_resync = 1;
        } else {
            err = response;
        }
    }

    private void schema_update_done(mapping my_schema, mapping schema, mixed ... results) {
        int err = 0;
        foreach (results;; array tmp) {
            [int ok, mixed response, mapping type] = tmp;

            if (!ok) {
                werror("Could not create field type: %O\n", type);
                err = 1;
            }
        }

        if (!err) update_fields(my_schema, schema);
    }

    array deep_and(array a, array b) {
        array ret = ({ });
        foreach (a;; mixed va) {
            foreach (b;; mixed vb) {
                if (equal(va, vb)) ret += ({ va });
            }
        }

        return ret;
    }

    private void update_fields(mapping my_schema, mapping schema) {
        mapping my_fields = mkmapping(my_schema->fields->name, my_schema->fields);
        mapping fields = mkmapping(schema->fields->name, schema->fields);

        object e = EventAggregator(fields_update_cb, my_schema, schema);

        foreach (my_fields - indices(fields);; mapping type) {
            core->schema->add_field(type, e->get_cb(), type);
        }

        /*
        foreach (fields - indices(my_fields);; mapping type) {
            if (type->name != "_version_") {
                core->schema->delete_field(type->name, e->get_cb(), type);
            }
        }
        */

        foreach (my_fields & indices(fields); string name; mapping type) {
            if (!equal(type, fields[name])) {
                core->schema->replace_field(type, e->get_cb(), type);
            }
        }

        array my_copy_fields = my_schema->copyFields;
        array copy_fields = schema->copyFields;

        foreach (my_copy_fields - deep_and(my_copy_fields, copy_fields);; mapping conf) {
            core->schema->add_copy_field(conf->source, conf->dest, e->get_cb(), conf);
        }

        foreach (copy_fields - deep_and(copy_fields, my_copy_fields);; mapping conf) {
            core->schema->delete_copy_field(conf->source, conf->dest, e->get_cb(), conf);
        }

        if (!sizeof(e)) synchronize_index();
        else needs_resync = 1;
    }

    private void fields_update_cb(mapping my_schema, mapping schema, mixed ... results) {
        int err = 0;
        foreach (results;; array tmp) {
            [int ok, mixed response, mapping type] = tmp;

            if (!ok) {
                werror("Could not create field %s: %O\n", type->name, response);
                err = 1;
            }
        }

        if (!err) synchronize_index();
    }

    object last_modification_filter();

    private void synchronize_index() {
        if (needs_resync || !last_modification_filter) {
            synchronize_all();
        } else {
            synchronize_filter(last_modification_filter());
        }
        werror("starting to synchronize index.\n");
    }

    private ADT.Queue sync_filters = ADT.Queue();
    private object sync_iterator;

    private void update_cb(int ok, mixed result) {
        if (ok) {
            do_sync();
        } else {
            werror("update failed: %O\n", result);
        }
    }

    private void commit_cb(int ok, mixed result) {
        if (!ok) {
            werror("Commit failed: %O\n", result);
        }
    }

    void sync_done() {
        core->commit(commit_cb);
        werror("Sync done.\n");
    }

    private void do_sync() {
        while (1) {
            object smart_type = table->smart_type;

            if (!smart_type->solr_transform) {
                error("%O is missing solr_transform callback.\n", smart_type);
            }

            if (sync_iterator) {
                if (sync_iterator->next()) {
                    array docs = sync_iterator->value();

                    docs = map(docs, smart_type->solr_transform);

                    if (sizeof(docs)) {
                        core->update(docs, update_cb);
                        return;
                    }
                }
            }

            if (sync_filters->is_empty()) {
                sync_done();
                return;
            }

            object filter = sync_filters->get();

            sync_iterator = table->PageIterator(filter, 0, 100);

            array docs = sync_iterator->value();

            if (!sizeof(docs)) {
                sync_iterator = 0;
                continue;
            }

            docs = map(docs, smart_type->solr_transform);

            core->update(docs, update_cb);
            return;
        }
    }

    void synchronize_filter(object filter) {
        sync_filters->put(filter);

        if (!sync_iterator) {
            do_sync();
        }
    }

    void synchronize_all() {
        // we can flush everything
        sync_filters->flush();
        synchronize_filter(SyncDB.MySQL.Filter.TRUE);
    }

    void create(.MySQL.TypedTable table, SolR.Collection core) {
        ::create(table, core);

        core->schema->retrieve(schema_cb);
    }
}
