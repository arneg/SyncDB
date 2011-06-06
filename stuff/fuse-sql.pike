#include <asm-generic/errno.h>

#define OPNOTSUPP(op) mixed op(mixed ... args) { \
    werror("%s(%O) not supported\n", #op, args); \
    return EOPNOTSUPP; \
}

SqlEventBridge sqlEventBridge;
Sql.Sql sql;
object Req = Serialization.Types.Tuple("_req", 0, Serialization.Types.Symbol(), Serialization.Types.Symbol(), SyncDB.Serialization.Schema);
mapping conns = ([ ]);
mapping(string:mapping(string:array(string))) tblschema = ([ ]);


class Fuser {
    inherit Fuse.Operations;

    mapping files = ([ ]), counts = ([ ]);
    function cb;

    int access(string path, int mode) {
	werror("access(%O, %O)\n", path, mode);
	return 0;
    }

    void create(array(string) argv, function cb) {
	this_program::cb = cb;
	Thread.Thread(Fuse.run, this, argv);
    }

#if 1
    OPNOTSUPP(chmod)
    OPNOTSUPP(chown)
    OPNOTSUPP(flush)
    OPNOTSUPP(tsync)
    OPNOTSUPP(getxattr)
    OPNOTSUPP(link)
    OPNOTSUPP(listxattr)
    OPNOTSUPP(mkdir)
    OPNOTSUPP(mknod)
    OPNOTSUPP(read)
    OPNOTSUPP(readlink)
    OPNOTSUPP(removexattr)
    OPNOTSUPP(rename)
    OPNOTSUPP(rmdir)
    OPNOTSUPP(setxattr)
    OPNOTSUPP(statfs)
    OPNOTSUPP(symlink)
    OPNOTSUPP(truncate)
    OPNOTSUPP(unlink)
    OPNOTSUPP(utime)
#endif

    Stdio.Stat|int(1..) getattr(string path) {
	Stdio.Stat ret = Stdio.Stat(file_stat("/tmp"));
	werror("getattr(%O(%O))\n", path, path = Stdio.simplify_path(path));
	switch(path) {
	case "/":
	    return ret;
	}

	if (counts[path]) {
	    ret->isdir = 0;
	    ret->isreg = 1;
	    return ret;
	}

	return 0;
    }

    int readdir(string path, function(string:void) cb) {
	cb(".");
	cb("..");

	return 0;
    }

    int creat(string path, int mode, int flag) {
	path = Stdio.simplify_path(path);
	werror("creat(%O, %O, %O)\n", path, mode, flag);

	if (!has_index(files, path)) {
	    files[path] = "";
	    ++counts[path];
	}
    }

    int write(string path, string data, int offset) {
	string file;
	path = Stdio.simplify_path(path);

	werror("write(%O, %O, %O)\n", path, data, offset);

	file = files[path];
	file = file[..offset - 1] + data + file[offset + sizeof(data)..];
	files[path] = file;

	return -sizeof(data);
    }

    int release(string path) {
	path = Stdio.simplify_path(path);

	werror("release(%O): %O\n", path, files[path]);

	if (!--counts[path]) {
	    call_out(cb, 0, path, m_delete(files, path));
	    m_delete(counts, path);
	}

	werror("release will return, i think it's ok.\n");

	return 1;
    }
}

class SqlEventBridge {
    class Table(string db, string table, SyncDB.Schema schema, function r, array(mixed) args) { 
	void reply(int id) {
	    call_out(r, 0, id, @args);
	}
    };

    mapping events = ([ ]);

    // db -> table -> (all schemas that care)
    mapping dbs = ([ ]);

    void create(Sql.Sql sql) {
    }

    void install_triggers(string db, string table) {
	sql->query("USE DATABASE %s", db);
	catch { sql->query(sprintf("DROP TRIGGER _syncdb_event_update_%s;", table)); };
	catch { sql->query(sprintf("DROP TRIGGER _syncdb_event_insert_%s;", table)); };
	sql->query(sprintf(#"CREATE TRIGGER _syncdb_event_update_%s
	    AFTER UPDATE ON %<s
	    FOR EACH ROW
	    BEGIN
		SELECT * INTO OUTFILE '/dev/shm/interSync/db_%<s' FROM %<s WHERE %<s.version = NEW.version;
	    END;
	", table));

	sql->query(sprintf(#"CREATE TRIGGER _syncdb_event_insert_%s
	    AFTER INSERT ON %<s
	    FOR EACH ROW
	    BEGIN
		SELECT * INTO OUTFILE '/dev/shm/interSync/db_%<s' FROM %<s WHERE %<s.version = NEW.version;
	    END;
	", table));
	if (!tblschema[db]) tblschema[db] = ([]);
	tblschema[db][table] = list_fields(db, table)->name;
    }

    array(mapping)|mapping query(string q) {
	werror("SQL: %s\n", q);
	return sql->query(q);
    }

    mixed register_event(string db, string table, SyncDB.Schema schema, function reply, mixed ... extra) {
	Table t = Table(db, table, schema, reply, extra);
	if (!dbs[db]) dbs[db] = ([]);
	if (!dbs[db][table]) {
	    install_triggers(db, table);
	    dbs[db][table] = ({ });
	}
	dbs[db][table] += ({ t });

	foreach (schema->m; string field; object type) {
	    if (type->is_link) {
		mapping tables = type->f_link->tables;
		foreach (tables; string tbl; string s) {
		    if (!dbs[db][tbl]) {
			install_triggers(db, tbl);
			dbs[db][tbl] = ({ });
		    }
		    dbs[db][tbl] += ({ ({ field, s, t }) });
		}
	    }
	}

	return t;
    }

    void unregister_event(mixed id) {
	// filter table

    }

    void trigger(string db, string table, mapping row) {
	if (dbs[db]) {
	    array schemas = dbs[db][table];

	    if (schemas) {
		foreach (schemas;; array|Table t) {
		    if (arrayp(t)) { // is a local change
			string id, fid;
			[id, fid, t] = t;
			query(sprintf("SELECT %s FROM %s WHERE %s=%s", t->schema->key, t->table, id, row[fid]));
		    } else {
			t->reply((int)row[t->schema->key]);
			continue;
		    }
		}
		// collect all tables and the fields therin
		// call some kind of compile function in schema, or something
		// send individual reply to the reply
	    }
	}
    }
}

array(array(string)) parse_rows(string s) {
    int mode;
    array rows = ({ }), fields = ({ });
    string field = "";

    for (int i; i < sizeof(s); ++i) {
	if (mode) {
	    mode = 0;
	    field += s[i..i];
	} else switch (s[i]) {
	    case '\\':
		mode = 1;
		break;
	    case '\t':
		fields += ({ field });
		field = "";
		break;
	    case '\n':
		fields += ({ field });
		rows += ({ fields });
		fields = ({ });
		field = "";
		break;
	    default:
		field += s[i..i];
	}
    }

    return rows;
}

void event(string table, string s) {
    array rows = parse_rows(s);
}

void reply( int id, Stdio.File con) {
    werror("update in : %d\n", id);
}


int rcb(mixed id, string data) {
    array a;
    conns[id]->feed(data);

    while (Serialization.Atom atom = conns[id]->parse()) {
	catch { 
	    SyncDB.Schema schema;
	    string db, table;

	    [db, table, schema] = Req->decode(atom);

	    sqlEventBridge->register_events(db, table, schema, reply, id);
	};
    }
    return 0;
}

int ccb(mixed id) {
    m_delete(conns, id);
    id->close();
    destruct(id);
}

int wcb(mixed id) {
    return 0;
}

int accept(mixed id) {
    Stdio.File conn = id->accept();

    conn->set_nonblocking(rcb, wcb, ccb);
    conn->set_id(conn);
    conns[conn] = Serialization.AtomParser();

    return 0;
}

array(mapping(string:mixed))|mapping(string:mixed) query(string sql) {
    werror("SQL: %s\n", sql);
    return this_program::sql->query(sql);
}

void sql_update(string path, string data) {
    string null, db, table;
    array(array(string))|mapping(string:string) rows;

    if (!has_prefix(path, "/db")) return;

    [null, db, table] = path / ".";


    rows = parse_rows(data);

    if (tblschema[db] && tblschema[db][table]) {
	catch {
	    rows = mkmapping(tblschema[db][table], rows[0]);
	    sqlEventBridge->trigger(db, table, rows);
	};
    }
}

array(mapping) list_fields(string db, string name) {
    query(sprintf("USE %s;", db));
    return sql->list_fields(name);
}

Fuser fuse;

int main(int argc, array(string) argv) {
    //Fuse.run(Fuser(), argv);
    
    sql = Sql.Sql("mysql://root@localhost/");
    sqlEventBridge = SqlEventBridge(sql);

    fuse = Fuser(argv, sql_update);
    
    Stdio.Port p = Stdio.Port(4012, accept);
    p->set_id(p);

    call_out(write, 1, "Thread created.\n");

    return -1;
}
