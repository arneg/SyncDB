#define LAZY(name, prog)                        \
    private object(prog) __ ## name;            \
    object(prog) name () {                      \
        if (!__ ## name) __ ## name = prog();     \
        return __ ## name;                       \
    }
    

class Base {
    constant is_schema_relevant = 1;

    int get_priority() {
        return 50;
    }

    // hide flag by default
    string encode_json() { return ""; }

    string sql_type(function(mixed:string) encode_sql) { return ""; }

    array(SyncDB.MySQL.Query) flag_definitions(object type) { return ({ }); }
}

class _Mandatory {
    inherit Base;

    constant is_mandatory = 1;

    string encode_json() {
        return "(new SyncDB.Flags.Mandatory())";
    }

    string sql_type(function(mixed:string) encode) {
        return "NOT NULL";
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("NOT NULL") });
    }
}

LAZY(Mandatory, _Mandatory)

class _Automatic {
    inherit Base;

    constant is_automatic = 1;
    constant is_not_null = 1;

    string encode_json() {
        return "(new SyncDB.Flags.Automatic())";
    }

    string sql_type(function(mixed:string) encode) {
        return "AUTO_INCREMENT";
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("AUTO_INCREMENT") });
    }
}

LAZY(Automatic, _Automatic);

class _Hidden {
    inherit Base;

    constant is_hidden = 1;
    constant is_schema_relevant = 0;
}

LAZY(Hidden, _Hidden)

class _Index {
    inherit Base;

    constant is_index = 1;

    string encode_json() {
        return "(new SyncDB.Flags.Index())";
    }
}

LAZY(Index, _Index)

class _Trivial {
    inherit Base;

    constant is_trivial = 1;
}

LAZY(Trivial, _Trivial)

class _Unique {
    inherit _Index;

    constant is_unique = 1;

    string encode_json() {
        return "(new SyncDB.Types.Unique())";
    }
}

LAZY(Unique, _Unique)

class _Key {
    inherit _Index;
    inherit _Unique;

    constant is_key = 1;
    constant is_not_null = 1;

    string encode_json() {
        return "(new SyncDB.Flags.Key())";
    }

    string sql_type(function(mixed:string) encode_sql) {
        return "PRIMARY KEY";
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("PRIMARY KEY") });
    }
}

LAZY(Key, _Key)

class _ReadOnly {
    inherit Base;

    constant is_writable = 0;

    string encode_json() {
        return "(new SyncDB.Flags.ReadOnly())";
    }
}

LAZY(ReadOnly, _ReadOnly)

class Range {
    inherit Base;
}

class Default {
    inherit Base;

    int is_default = 1;

    mixed default_value;

    void create(mixed v) {
        default_value = v;
    }

    string encode_json() {
        return "(new SyncDB.Flags.Default())";
    }

    string sql_type(function(mixed:string) encode) {
        return sprintf("NOT NULL");
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("NOT NULL") });
    }

    string _sprintf(int t) {
        return sprintf("%O(%O)", this_program, default_value);
    }
}

class MaxLength {
    inherit Base;

    constant is_maxlength = 1;

    int length;

    void create(int len) {
        length = len;
    }

    mixed cast(string s) {
        if (s == "int") return length;
        error("Cannot cast %O to %O.\n", this, s);
    }
}

class Charset {
    inherit Base;

    constant is_charset = 1;

    string charset_name;

    void create(string charset_name) {
        this_program::charset_name = charset_name;
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("CHARSET "+charset_name) });
    }
}

class Collate {
    inherit Base;

    constant is_collate = 1;

    string charset_name;

    void create(string charset_name) {
        this_program::charset_name = charset_name;
    }

    array(SyncDB.MySQL.Query) flag_definitions(object type) {
        return ({ SyncDB.MySQL.Query("COLLATE "+charset_name) });
    }
}

class _WriteOnly {
    inherit Base;

    constant is_readable = 0;

    string encode_json() {
        return "(new SyncDB.Types.WriteOnly())";
    }
}

LAZY(WriteOnly, _WriteOnly);

class SolRField {
    inherit Base;

    constant is_schema_relevant = 0;

    array(mapping) fields;

    void create(mapping ... fields) {
        this_program::fields = fields;
    }

    void add_solr_fields(mapping m, mapping def) {
        foreach (fields;; mapping field) {
            if (def) field = def + field;
            m[field->name] = field;
        }
    }
}

class SolRSettings {
    inherit Base;

    constant is_schema_relevant = 0;

    mapping settings;

    void create(mapping settings) {
        this_program::settings = settings;
    }

    mapping get_solr_settings() {
        return settings;
    }
}
