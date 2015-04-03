inherit .DoubleEncodedString;

object previous_type() {
    return .DoubleEncodedString(name, @_flags);
}

string encode_sql_value(mixed val) {
    if (length) return val;
    return string_to_utf8(val);
}

string decode_sql_value(string s) {
    if (length) return s;
    return utf8_to_string(s);
}

void generate_encode_value(object buf, string val) {
    if (length) buf->add("%s", val);
    else buf->add("%H(%s)", string_to_utf8, val);
}

void generate_decode_value(object buf, string val) {
    if (length) buf->add("%s", val);
    else buf->add("%H(%s)", utf8_to_string, val);
}
