final array(Field) fields = ({});
final mapping(string:object(Field)) nfields;
final object schema;

protected program datum = .Datum;
protected program real_datum;
protected program table_program = .TypedTable;

array(mapping(string:object)) changes;

program get_datum() {
    return real_datum;
}

void compile_datum(object gen, object blueprint) {
    gen->add("inherit %H;\n", datum);

    // version getter
    gen->Getter("version")->add("return _modified[%O] || _data[%<O];\n", "version");

    fields->compile_datum(gen, blueprint);
}

void before_insert(object(.Table) table, mapping row);
void after_insert(object(.Table) table, mapping row);
void before_update(object(.Table) table, mapping row, mapping changes);
void after_update(object(.Table) table, mapping row, mapping changes);
void before_delete(object(.Table) table, mapping keys);
void after_delete(object(.Table) table, mapping keys);

object get_previous_table(function(void:Sql.Sql) get_sql, string name,
                          int schema_version, mapping(string:int) type_versions) {
    object schema = this_program::schema->get_previous_schema(schema_version, type_versions);
    object table = table_program(name, get_sql, schema, name, this);

    foreach (({ "before_insert", "after_insert", "before_update",
                "after_update", "before_delete", "after_delete" });; string trigger) {
        function fun = predef::`->(this, trigger);
        if (fun) table->register_trigger(trigger, fun);
    }

    return table;
}

object get_table(function(void:Sql.Sql) get_sql, string name) {
    object table = table_program(name, get_sql, schema, name, this);

    foreach (({ "before_insert", "after_insert", "before_update",
                "after_update", "before_delete", "after_delete" });; string trigger) {
        function fun = predef::`->(this, trigger);
        if (fun) table->register_trigger(trigger, fun);
    }
    
    return table;
}

void create_table(function(void:Sql.Sql) get_sql, string name) {
    .create_table(get_sql(), name, schema);
}

protected class Field {
    mixed syncdb_class;
    array flags;
    string name;

    void create(program syncdb_class, mixed ... flags) {
        this_program::syncdb_class = syncdb_class;
        this_program::flags = flags;
    }

    void compile_datum(object gen, object blueprint) {
        if (has_value(indices(blueprint), name)) return;
        gen->Getter(name)->add("return _modified[%O] || _data[%<O];\n", name);
        gen->Setter(name)->add("check_value(%O, v); return _modified[%<O] = v;\n", name);
    }
    
    object syncdb_type() {
        return syncdb_class(name, @flags);
    }
};

mixed unique_identifier(mapping|object row) {
    return row[schema->key];
}

void create() {
    int i = 0;
    array(string) ind = call_function(::_indices, 3);

    foreach (ind;; string name) {
        if (has_prefix(name, "`")) {
            ind -= ({ name, name[1..] });
        }
    }

    foreach (ind;; string name) {
        mixed val = call_function(::`->, name, this);
        if (objectp(val) && Program.inherits(object_program(val), Field)) {
            val->name = name;
            fields += ({ val });
        }
    }

    schema = SyncDB.Schema(@fields->syncdb_type());

    nfields = mkmapping(fields->name, fields);

    foreach (fields;; object f) {
        if (f->fields) {
            foreach (f->fields;; object subfield) {
                nfields[f->name + "." + subfield->name] = subfield;
            }
        }
    }

    if (changes) {
        mapping current_fields = nfields;
        array schemata = allocate(sizeof(changes) + 1);

        for (int i = sizeof(changes)-1; i >= 0; i--) {
            mapping m = changes[i];

            foreach (m; string name; object field) if (field) field->name = name;

            // overwrite with _previous_ fields
            m = current_fields + m;

            foreach (m; string name; object field) if (!field) m_delete(m, name);

            // ordering is lost here, is this a problem?
            // FIXME: we can win back the ordering, by extracting the order from within
            // fields, i.e. use filter(fields, mkmapping(values(m), values(m)));
            current_fields = m;

            schemata[i] = SyncDB.Schema(@values(current_fields)->syncdb_type());
        }

        schemata[-1] = schema;

        array migrations = allocate(sizeof(schemata)-1);

        for (int i = 1; i < sizeof(schemata); i++) {
            object schema = schemata[i];
            migrations[i-1] = SyncDB.Migration.Base(schemata[i-1], schema);
        }

        schema->migrations = migrations;
    }

    // compile datum
    object gen = SyncDB.Code.Program();
    compile_datum(gen, datum());
    // werror("Compiling %O:\n%s\n==========\n", this_program, (string)gen->buf);
    real_datum = gen->compile(sprintf("Datum<%O>", this_program));
}

#define MAP_TYPE(name)    object name (mixed ... flags) {       \
    return Field(SyncDB.Types. ## name, @flags);            \
}

MAP_TYPE(Integer)
MAP_TYPE(String)
MAP_TYPE(JSON)
MAP_TYPE(Datetime)
MAP_TYPE(Date)
MAP_TYPE(Time)
MAP_TYPE(Enum)
MAP_TYPE(Float)

#define MAP_FLAG(name, rname)   object name (mixed ... args) {  \
    return SyncDB.Flags. ## rname (@args);                  \
}

MAP_FLAG(MAX_LENGTH, MaxLength)
MAP_FLAG(DEFAULT, Default)

#define MAP_CFLAG(name, rname)   protected object name = SyncDB.Flags. ## rname ( );

MAP_CFLAG(INDEX, Index)
MAP_CFLAG(KEY, Key)
MAP_CFLAG(AUTOMATIC, Automatic)
MAP_CFLAG(UNIQUE, Unique)
MAP_CFLAG(HIDDEN, Hidden)
