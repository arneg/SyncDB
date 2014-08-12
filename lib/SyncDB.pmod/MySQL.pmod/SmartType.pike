final protected array(Field) _fields = ({});

protected program datum = .Datum;

program get_datum() {
    return datum;
}

array `fields() {
    return .get_fields(this_program);
}

mapping `nfields() {
    return .get_nfields(this_program);
}

private object _schema;

object `schema() {
    if (!_schema) _schema = .get_schema(this_program);
    return _schema;
}

object get_table(function(void:Sql.Sql) get_sql, string name, void|function|program prog) {
    return .TypedTable(name, get_sql, schema, name, this);
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
        _fields += ({ this });
    }
    
    object syncdb_type() {
        return syncdb_class(name, @flags);
    }
};

mixed unique_identifier(mapping|object row) {
    return row[schema->key];
}

void create() {
    if (schema) return;

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
        }
    }

    // remove overloaded ones
    _fields = filter(_fields, _fields->name);

    .set_schema(this_program, SyncDB.Schema(@_fields->syncdb_type()));
    .set_fields(this_program, _fields);
}

#define MAP_TYPE(name)    object name (mixed ... flags) {       \
    if (!_fields) return 0;  \
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
    if (!_fields) return 0;  \
    return SyncDB.Flags. ## rname (@args);                  \
}

MAP_FLAG(MAX_LENGTH, MaxLength)
MAP_FLAG(DEFAULT, Default)

#define MAP_CFLAG(name, rname)   object ` ## name ( ) {     \
    if (!_fields) return 0;  \
    return SyncDB.Flags. ## rname ( );                  \
}

MAP_CFLAG(INDEX, Index)
MAP_CFLAG(KEY, Key)
MAP_CFLAG(AUTOMATIC, Automatic)
MAP_CFLAG(UNIQUE, Unique)
MAP_CFLAG(HIDDEN, Hidden)
