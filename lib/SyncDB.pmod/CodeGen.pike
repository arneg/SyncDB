String.Buffer buf = String.Buffer();

protected mapping symbols = ([]);

string get_id() {
    for (int i = sizeof(symbols);; i++) {
        string s = sprintf("sym__%d", i);
        if (!has_index(symbols, s)) return s;
    }
}

mixed resolv(string idx) {
    // TODO: UNDEFINED ?
    return has_index(symbols, idx) ? symbols[idx] : master()->resolv(idx);
}

void create() {
}

private class F(mixed arg) {
    string _sprintf(int t) {
        if (t == 'H') {
            string n = get_id();
            symbols[n] = arg;
            return n;
        } else if (t == 'c') {
            return sprintf("%O", arg);
        }

        return sprintf((string)({ '%', t }), arg);
    }
}

void add(string fmt, mixed ... args) {
    if (sizeof(args)) buf->sprintf(fmt, @map(args, F));
    else buf->add(fmt);
}

program compile(void|string filename) {
    string code = buf->get();
    return compile_string(code, filename, this);
}
