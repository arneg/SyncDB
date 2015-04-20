string _sql_path;

string `sql_path=(string v) {
    _sql_path = v;
#ifdef CACHE_CONNECTIONS
    _sql = Thread.Local();
#endif
}

string `sql_path() {
    return _sql_path;
}


#ifdef CACHE_CONNECTIONS
Thread.Local _sql = Thread.Local();
#endif

class SQLKey(Sql.Sql con) {
    mixed `->(string key) {
        return predef::`->(con, key);
    }

    protected void destroy(int reason) {
        switch (reason) {
        case Object.DESTRUCT_EXPLICIT:
            werror("explicit destruct.\n");
            break;
        case Object.DESTRUCT_NO_REFS:
            //werror("refcount destruct.\n");
            break;
        case Object.DESTRUCT_GC:
            werror("gc destruct.\n");
            break;
        case Object.DESTRUCT_CLEANUP:
            werror("cleanup destruct.\n");
            break;
        }
    }
}

Sql.Sql `sql() {
    Sql.Sql _sql;
#ifdef CACHE_CONNECTIONS
    _sql = this_program::_sql->get();

    // do not hand the connection out if it is being
    // used somewhere else

    if (!_sql || _refs(_sql) > 3 || !_sql->is_open())
#endif
    {
        _sql = Sql.Sql(_sql_path);
        _sql->set_charset("unicode");
#ifdef CACHE_CONNECTIONS
        this_program::_sql->set(_sql);
#endif
    }

    return SQLKey(_sql);
}

mixed get_sample_data(string type_name, int n, void|object type) {
    mixed v;

    if (type && type->is["default"] && n & 1) {
        // test default values.
        return Val.null;
    }

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
        if (type == a->version) continue;
        program prog = object_program(type);
        string name = type->name;
        mixed v;

        v = get_sample_data(type->type_name(), n, type);
    
        data[name] = v;
    }

    return data;
}

void run(string path, string name, function(mixed...:void) r, mixed ... args) {
    sql_path = path;

    catch (sql->query("DROP DATABASE `migration_test`"));

    sql->query("CREATE DATABASE `migration_test`");

    sql_path = path + "migration_test";

    werror("Running test %s ... ", name);

    mixed err;

    int t1 = gethrtime();
    float t = gauge {
        err = catch(r(@args));
    };
    int t2 = gethrtime();
    float t_tot = (t2 - t1)/ 1E6;

    if (err) {
        werror(" ERR %f seconds (utime: %f seconds)\n", t_tot, t);
        error_count++;
        master()->handle_error(err);
    } else {
        werror("  OK %f seconds (utime: %f seconds)\n", t_tot, t);
        success_count++;
    }
    //gc();
}

variant void run(string path, function(mixed...:void) r, mixed ... args) {
    run(path, sprintf("%O", r), r, @args);
}

int success_count, error_count;

int main(int argc, array(string) argv) {
    string path = argv[1];

    foreach (sort(indices(this));; string s) {
        if (has_prefix(s, "_test")) {
            if (argc > 2 && !has_value(argv[2..], s)) continue;
            mixed v = predef::`->(this, s);
            if (functionp(v)) run(path, v);
        }
    }

    int all = success_count + error_count;

    werror("%d tests failed.\n%d tests succeeded\n", error_count, success_count);

    return 0;
}
