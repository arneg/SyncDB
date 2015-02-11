constant is_readable = 1;
constant is_writable = 1;

protected array(SyncDB.Flags.Base) _flags;
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

string sql_name(string table) {
    object f = flags->foreign;
    if (f) {
	return f->table||table + "." + f->field||name;
    }
    return table + "." + name;
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

string _sprintf(int t) {
    return sprintf("%O(%O, %s)", this_program, name, map(_flags, Function.curry(sprintf)("%O")) * ", ");
}
