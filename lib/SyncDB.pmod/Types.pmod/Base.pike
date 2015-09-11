constant is_readable = 1;
constant is_writable = 1;

array(this_program) get_column_fields() {
    return ({ this });
}

array(SyncDB.Flags.Base) _flags;
mapping(string:SyncDB.Flags.Base) flags = ([]);
mapping(string:int(0..1)) is = ([
    "readable" : 1,
    "writable" : 1,
]);
string name;

#define MAP(name)     mixed ` ## name ( ) {                           \
    return Function.curry(SyncDB.MySQL.Filter.## name)(this);   \
}

MAP(Or)
MAP(And)
MAP(Equal)
MAP(Ne)
MAP(In)
MAP(Match)
MAP(True)
MAP(False)
MAP(Gt)
MAP(Ge)
MAP(Lt)
MAP(Le)

#undef MAP


void get_default(mapping def) {
    object f = flags["default"];

    if (f) {
	def[name] = f->default_value;
    }
}

void create(string name, SyncDB.Flags.Base ... _flags) {
    this_program::name = name;
    this_program::_flags = _flags;

    foreach (_flags;; object f) {
        foreach (indices(f);; string s) {
            if (has_prefix(s, "is_")) {
                string n = s[3..];
                flags[n] = f;
                is[n] = f[s];
            }
        }
    }
}

mixed decode_sql(string table, mapping row, mapping|void new);
mapping encode_sql(string table, mapping row, mapping new);

string sql_name(void|string table) {
    if (table) {
        object f = flags->foreign;
        if (f) {
            return f->table||table + "." + f->field||name;
        }
        return table + "." + name;
    } else {
        return name;
    }
}

string escaped_sql_name(void|string table) {
    if (table) {
        object f = flags->foreign;
        if (f) {
            return sprintf("`%s`.`%s`", f->table||table, f->field||name);
        }
        return "`" + table + "`.`" + name + "`";
    } else {
        return "`" + name + "`";
    }
}

array(string) escaped_sql_names(string table) {
    return ({ escaped_sql_name(table) });
}

array(string) sql_names(string table) {
    return ({ sql_name(table) });
}

string encode_json(string p, void|array extra) {
    if (is->hidden) return "";
    if (!extra) extra = ({});
    extra = ({ Standards.JSON.encode(name) }) + extra + filter(map(_flags, Standards.JSON.encode), sizeof);
    return sprintf("(new %s(%s))", p, extra * (",\n"+" "*8));
}

#if constant(Serialization)
object get_parser();

object _parser;
object parser() {
    if (!_parser) {
	_parser = get_parser() |
#ifdef TEST_RESOLVER
		SyncDB.Serialization.Null;
#else
		master()->resolv("SyncDB.Serialization.Null");
#endif
	if (!is->mandatory) {
	    _parser |= Serialization.Types.Undefined;
	}
    }
    return _parser;
}

object get_filter_parser() {
    //return Serialization.Types.Bloom();
#ifdef TEST_RESOLVER
    return SyncDB.Serialization.BloomFilter(MMP.Utils.Bloom.SHA256);
#else
    return master()->resolv("SyncDB.Serialization.BloomFilter")(MMP.Utils.Bloom.SHA256);
#endif
}
#endif

string sql_type(Sql.Sql sql, void|string type);

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb);

string _sprintf(int t) {
    string f = map(_flags, Function.curry(sprintf)("%O")) * ", ";
    if (sizeof(f)) f = ", "+f;
    return sprintf("%O(%O%s)", this_program, name, f);
}

int(0..1) _equal(mixed b) {
    return objectp(b) && object_program(b) == this_program && equal(_flags, b->_flags);
}

//! Similar to @[_equal] but ignore those flags, which are not relevant for the schema.
int(0..1) schema_equal(mixed b) {
    return objectp(b) && object_program(b) == this_program &&
           equal(filter(_flags, _flags->is_schema_relevant),
                 filter(b->_flags, b->_flags->is_schema_relevant));
}

string type_name();

object previous_type();

int type_version() {
    if (previous_type) return previous_type()->type_version() + 1;
    return 0;
}

object get_previous_type(string type_name, int version) {
    if (!this_program::type_name || this_program::type_name() != type_name) return this;
    if (type_version() > version) return previous_type()->get_previous_type(type_name, version);
    return this;
}

//! Get the migration object necessary to migrate from @[previous_type()@]. @expr{from@} is expected to
//! contain types of @[previous_type()@].
object get_migration(string type_name, object from, object to) {
    if (type_name != this_program::type_name()) return 0;
    if (!previous_type) return 0;
    return get_migration_program()(from, to);
}

program get_migration_program() {
    return  master()->resolv("SyncDB.Migration.Simple");
}

void type_versions(mapping(string:int) versions) {
    versions[type_name()] = type_version();
}

int(0..1) supports_native_default() {
    return 1;
}
