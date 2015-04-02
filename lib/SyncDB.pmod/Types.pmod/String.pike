inherit .Simple;

int length;

#if constant(Serialization)
object get_parser() {
    return Serialization.Types.String();
}
#endif

#if constant(ADT.CritBit)
program get_critbit() {
    return ADT.CritBit.Tree;
}
#endif

string encode_sql_value(mixed val) {
    return string_to_utf8(val);
}

string decode_sql_value(string s) {
    return utf8_to_string(s);
}

void generate_encode_value(object buf, string val) {
    buf->add("%H(%s)", string_to_utf8, val);
}

void generate_decode_value(object buf, string val) {
    buf->add("%H(%s)", utf8_to_string, val);
}

string encode_json(string|void type) {
    return ::encode_json(type || "SyncDB.Types.String");
}

string sql_type(Sql.Sql sql) {
    if (length) {
	return ::sql_type(sql, sprintf("VARCHAR(%d)", length));
    } else {
	return ::sql_type(sql, "LONGTEXT BINARY");
    }
}

array(SyncDB.MySQL.Query) column_definitions(void|function(object:int(0..1)) filter_cb) {
    string sql_type;
    if (length) sql_type = sprintf("VARCHAR(%d)", length);
    else sql_type = "LONGTEXT BINARY";
    return ::column_definitions(sql_type, filter_cb);
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
