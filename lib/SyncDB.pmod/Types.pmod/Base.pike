constant is_readable = 1;
constant is_writable = 1;

array(SyncDB.Flags.Base) flags;
int priority = 50;
string name;
string table;

constant _types = ([
    "name" : "string",
    "table" : -1,
    "priority" : -1,
    "_parser" : -1
]);

mixed `->(string index) {
    if (has_prefix(index, "is_")) {
	array(int(0..1)) is = filter(flags[index], lambda(mixed m) { return !zero_type(m); });
	if (sizeof(is)) return max(@is);
	return this[index];
    } else if (has_prefix(index, "f_")) {
	array t = filter(flags + ({ this }), lambda(object o) { return o["is_"+index[2..]]; });
	if (!sizeof(t)) {
	    return UNDEFINED;
	}
	return t[0];
    } else if (has_index(SyncDB.MySQL.Filter, index)) {
	return Function.curry(SyncDB.MySQL.Filter[index])(this);
    }
    return call_function(::`->, index, this);
}

void get_default(mapping def) {
    object f = this->f_default;

    if (f) {
	def[name] = f->default_value;
    }
}

void create(string name, SyncDB.Flags.Base ... flags) {
    this_program::name = name;
    this_program::flags = flags;
}

mixed decode_sql_value(string s) {
    return s;
}

string encode_sql_value(mixed v) {
    return v;
}

mixed decode_sql(string table, mapping row, mapping|void new) {
    string n = sql_name(table);
    mixed v;
    if (has_index(row, n)) {
	v = row[n];
	if (stringp(v)) {
	    v = decode_sql_value(v);
	} else v = Val.null;
	if (new) new[name] = v;
	return v;
    }
    return UNDEFINED;
}

mapping encode_sql(string table, mapping row, mapping|void new) {
    if (!new) new = ([]);
    if (has_index(row, name)) {
	new[escaped_sql_name(table)] = (row[name] == Val.null)
				? Val.null
				: encode_sql_value(row[name]);
    }
    return new;
}

// TODO: this can be cached
string sql_name(string table) {
    object f = this->f_foreign;
    if (f) {
	return sprintf("%s.%s", f->table||table, f->field||name);
    }
    return sprintf("%s.%s", table, name);
}

string escaped_sql_name(void|string table) {
    if (table) {
        object f = this->f_foreign;
        if (f) {
            return sprintf("`%s`.`%s`", f->table||table, f->field||name);
        }
        return sprintf("`%s`.`%s`", table, name);
    } else {
        return sprintf("`%s`", name);
    }
}

array(string) escaped_sql_names(string table) {
    return ({ escaped_sql_name(table) });
}

array(string) sql_names(string table) {
    return ({ sql_name(table) });
}

string encode_json(string p, void|array extra) {
    if (this->is_hidden) return "";
    if (!extra) extra = ({});
    extra = ({ Standards.JSON.encode(name) }) + extra + filter(map(flags, Standards.JSON.encode), sizeof);
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
	if (!this->is_mandatory) {
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

string sql_type(Sql.Sql sql, void|string type) {
    if (type) 
	return sprintf("`%s` %s %s", name, type, flags->sql_type(encode_sql_value) * " ");
    else return 0;
}

string _sprintf(int t) {
    return sprintf("%O(%O, %s)", this_program, name, map(flags, Function.curry(sprintf)("%O")) * ", ");
}
