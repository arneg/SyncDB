inherit .Base;

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

mapping encode_sql(string table, mapping row, mapping new) {
    if (!new) new = ([]);
    if (has_index(row, name)) {
        mixed v = row[name];
	new[escaped_sql_name(table)] = (objectp(v) && v->is_val_null)
				? v
				: encode_sql_value(v);
    }
    return new;
}

void generate_decode_value(object buf, string val);
void generate_encode_value(object buf, string val);

void generate_encode(object buf, string table) {
    string sql_name = escaped_sql_name(table);
    buf->add(" if (has_index(row, %c)) {\n", name);
    buf->add(" mixed v = row[%c];", name);
    buf->add(" if (objectp(v) && v->is_val_null) new[%c] = v;", sql_name);
    buf->add(" else new[%c] = ", sql_name);
    if (generate_encode_value) {
        generate_encode_value(buf, "v");
    } else {
        buf->add("%H(row[%c])", encode_sql_value, name);
    }
    buf->add(";\n");
    buf->add(" }\n");
}

void generate_decode(object buf, string table) {
    buf->add(" v = row[%c]; ", sql_name(table));
    if (!is->not_null) buf->add("if (stringp(v)) ");
    buf->add("new[%c] = ", name);
    if (generate_decode_value) {
        generate_decode_value(buf, "v");
    } else {
        buf->add("%H(row[%c])", decode_sql_value, sql_name(table));
    }
    buf->add(";\n");
}

string sql_type(Sql.Sql sql, void|string type) {
    if (type) 
	return sprintf("`%s` %s %s", name, type, _flags->sql_type(encode_sql_value) * " ");
    else return 0;
}

array(SyncDB.MySQL.Query) column_definitions(object(SyncDB.MySQL.Query)|string sql_type,
                                             void|function(object:int(0..1)) filter_cb) {
    array(SyncDB.MySQL.Query) flag_definitions = Array.flatten((filter_cb ? filter(_flags, filter_cb) : _flags)->flag_definitions(this));
    flag_definitions = map(flag_definitions, predef::`+, " ");
    return ({ predef::`+(SyncDB.MySQL.Query(sprintf("`%s` ", name)), sql_type, " ", @flag_definitions) });
}

