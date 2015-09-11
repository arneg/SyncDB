inherit .Simple;

int length;

object previous_type() {
    return .DoubleEncodedString(name, @_flags);
}

string encode_sql_value(mixed val) {
    return val;
}

string decode_sql_value(string s) {
    return s;
}

void generate_encode_value(object buf, string val) {
    buf->add("%s", val);
}

void generate_decode_value(object buf, string val) {
    buf->add("%s", val);
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    string sql_type;
    if (length) sql_type = sprintf("VARCHAR(%d)", length);
    else sql_type = "LONGTEXT";
    return ::column_definitions(sql_type, filter_cb);
}

string sql_type(Sql.Sql sql) {
    if (length) {
	return ::sql_type(sql, sprintf("VARCHAR(%d)", length));
    } else {
	return ::sql_type(sql, "LONGTEXT");
    }
}

void create(string name, mixed ... args) {
    if (sizeof(args) && intp(args[0]) && args[0] > 0) {
        args[0] = SyncDB.Flags.MaxLength(args[0]);
    }

    ::create(name, @args);

    length = (int)this->flags->maxlength;
}

string type_name() {
    return "string";
}

int(0..1) supports_native_default() {
    return !!length;
}
