string _sql_path;

string `sql_path=(string v) {
    _sql_path = v;
    _sql = 0;
}

string `sql_path() {
    return _sql_path;
}

Sql.Sql _sql;

Sql.Sql `sql() {
    if (!_sql || !_sql->is_open()) {
        _sql = Sql.Sql(_sql_path);
    }

    return _sql;
}

mapping sample_data(SyncDB.Schema a, int n) {
    mapping data = ([]);

    foreach (a;; object type) {
        if (type->is->automatic) continue;
        program prog = object_program(type);
        string name = type->name;
        mixed v;

        switch (type->type_name()) {
        case "string":
            v = (string)n;
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
        case "json":
            v = ([ (string)n : ({ n, n+1 }) ]);
            break;
        default:
            v = Val.null;
            break;
        }
    
        data[name] = v;
    }

    return data;
}
