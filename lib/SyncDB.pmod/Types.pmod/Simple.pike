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
    buf->add(" v = row[%c];", name);
    buf->add(" if (stringp(v)) new[%c] = ", escaped_sql_name(table));
    if (generate_encode_value) {
        generate_encode_value(buf, "v");
    } else {
        buf->add("%H(row[%c])", encode_sql_value, name);
    }
    buf->add(";\n");
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

