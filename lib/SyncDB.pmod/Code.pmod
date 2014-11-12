class Base(void|object parent) {
    string get_id(mixed val) {
        return parent->get_id(val);
    }

    array(this_program|string) data = ({ });

    private class F(mixed arg) {
        string _sprintf(int t) {
            if (t == 'H') {
                return get_id(arg);
            } else if (t == 'c') {
                return sprintf("%O", arg);
            }

            return sprintf((string)({ '%', t }), arg);
        }
    }

    void add(string fmt, mixed ... args) {
        if (sizeof(args)) fmt = sprintf(fmt, @map(args, F));

        data += ({ fmt });
    }

    void render(String.Buffer buf) {
        foreach (data;; string|this_program o) {
            if (stringp(o)) buf->add(o);
            else o->render(buf);
        }
    }

    string _sprintf(int type) {
        if (type == 'O') {
            String.Buffer buf = String.Buffer();
            buf->add("Base():\n");
            render(buf);
            buf->add("\n)\n");
            return buf->get();
        } else {
            return sprintf("%O()", this_program);
        }
    }

    void clear() {
        data = ({ });
    }
}

class Scope(void|object parent) {
    inherit Base;

    mapping symbols = ([]);

    mixed get_symbol(string name) {
        return symbols[name];
    }

    void set_symbol(string name, mixed v) {
        if (has_index(symbols, name)) error("Method %O already defined.\n", name);
        symbols[name] = v;
    }
}

class Method(void|object parent, string type, string name, void|string args) {
    inherit Scope;

    void render(String.Buffer buf) {
        buf->sprintf("%s %s(%s) {\n", type, name, args||"");
        ::render(buf);
        buf->add(" }\n");
    }
}

class Program {
    inherit Scope;

    protected mapping global_symbols = ([]);

    mixed lookup(string name) {
        return global_symbols[name];
    }

    string get_id(mixed val) {
        for (int i = sizeof(global_symbols);; i++) {
            string s = sprintf("sym__%d", i);
            if (!lookup(s)) {
                global_symbols[s] = val;
                return s;
            }
        }
    }

    mixed resolv(string idx) {
        return has_index(global_symbols, idx) ? lookup(idx) : master()->resolv(idx);
    }

    object Method(string type, string name, void|string args) {
        object m = global::Method(this, type, name, args);
        set_symbol(name, m);
        data += ({ m });
        return m;
    }

    object Setter(string name, void|string type) {
        if (!type) type = "mixed";
        return Method(type, "`"+name+"=", type + " v");
    }

    object Getter(string name, void|string type) {
        if (!type) type = "mixed";
        return Method(type, "`"+name);
    }

    program compile(void|string filename) {
        String.Buffer buf = String.Buffer();

        render(buf);

        string code = buf->get();

        mixed err = catch {
            return compile_string(code, filename, this);
        };

        if (err) {
            werror("Error compiling %O:\n====\n%s\n====\n", this, code);
            throw(err);
        }
    }
}
