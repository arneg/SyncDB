final protected array(Field) _fields = ({});

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

void create() {
    array fields = _fields;
    _fields = 0;
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
    fields = filter(fields, fields->name);

    .set_schema(this_program, SyncDB.Schema(@fields->syncdb_type()));
    .set_fields(this_program, fields);
}

#define MAP_TYPE(name)    object name (mixed ... flags) {       \
    if (schema) return 0;  \
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
    if (schema) return 0;  \
    return SyncDB.Flags. ## rname (@args);                  \
}

MAP_FLAG(MAX_LENGTH, MaxLength)
MAP_FLAG(DEFAULT, Default)

#define MAP_CFLAG(name, rname)   object ` ## name ( ) {     \
    if (schema) return 0;  \
    return SyncDB.Flags. ## rname ( );                  \
}

MAP_CFLAG(INDEX, Index)
MAP_CFLAG(KEY, Key)
MAP_CFLAG(AUTOMATIC, Automatic)
MAP_CFLAG(UNIQUE, Unique)
MAP_CFLAG(HIDDEN, Hidden)
