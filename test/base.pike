string _sql_path;

string `sql_path=(string v) {
    _sql_path = v;
    _sql = Thread.Local();
}

string `sql_path() {
    return _sql_path;
}


Thread.Local _sql = Thread.Local();

Sql.Sql `sql() {
    Sql.Sql _sql = this_program::_sql->get();

    // do not hand the connection out if it is being
    // used somewhere else

    if (!_sql || _refs(_sql) > 3 || !_sql->is_open()) {
        _sql = Sql.Sql(_sql_path);
        _sql->set_charset("unicode");
        this_program::_sql->set(_sql);
    }

    return _sql;
}

mixed get_sample_data(string type_name, int n, void|object type) {
    mixed v;

    switch (type_name) {
    case "string":
        if (type && type->is->unique) {
            // the below might clash with collation
            v = (string)n;
        } else v = (string)enumerate(10, 1, 'A'+n);
        break;
    case "integer":
        v = n;
        break;
    case "datetime":
        v = Calendar.Second("unix", n);
        break;
    case "date":
        v = Calendar.Second("unix", n)->day();
        break;
    case "float":
        v = (float)n;
        break;
    case "json":
        v = ([ (string)n : ({ n, n+1 }) ]);
        break;
    case "vector":
        if (type) {
            array ret = allocate(sizeof(type->fields));
            foreach (type->fields; int i; object field) {
                ret[i] = get_sample_data(field->type_name, n, field);
            }
            return ret;
        }
        break;
    default:
        v = Val.null;
        break;
    }

    return v;
}

mapping sample_data(SyncDB.Schema a, int n) {
    mapping data = ([]);

    foreach (a;; object type) {
        if (type->is->automatic) continue;
        program prog = object_program(type);
        string name = type->name;
        mixed v;

        v = get_sample_data(type->type_name(), n, type);
    
        data[name] = v;
    }

    return data;
}
