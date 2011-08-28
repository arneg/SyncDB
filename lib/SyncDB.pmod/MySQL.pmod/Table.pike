// vim:syntax=lpc
inherit SyncDB.Table;

Sql.Sql sql;
string table;
Table table_o;
SyncDB.Version version;

/*
 * JOIN (using INNER JOIN)
 *  - join id in secondary table has to be automatic
 *    either explicit or doing it by hand/with triggers
 *  - join id in main table is read-only
 *  - insert new row in secondary tables on insert
 * LINK (using JOIN)
 *  - join id in main table can only be set to NULL or 
 *    an existing id in the other table
 *  - join id is rw
 * REFERENCE (using JOIN) inherits LINK
 *  - members in secondary table are read-only
 *    and optional.
 */

#define DB_DEBUG
mixed query(mixed ... args) {
    string s = sprintf(@args);
#ifdef DB_DEBUG
    werror("SQL:\t%s\n", s);
#endif
    return sql->query(s);
}

string mapping_implode(mapping m, string s1, string s2) {
    array(string) t = allocate(sizeof(m));
    int i = 0;
    foreach (m; string n; string v) {
	t[i++] = n + s1 + v;
    }
    return t*s2;
}

class Table {
    array(SyncDB.Types.Base) fields = ({});
    mapping sql_schema = ([]);

    string name;

    void create(string name) {
	this_program::name = name;
	array a = sql->list_fields(name);
	foreach (a;; mapping m) {
	    sql_schema[m->name] = m;
	}
    }

    void add_field(object type) {
	fields += ({ type });
    }

    array(string) field_names() {
	return fields->name;
    }

    array(SyncDB.Types.Base) readable() {
	return filter(fields, fields->is_readable);
    }

    array(SyncDB.Types.Base) writable() {
	return filter(fields, fields->is_writable);
    }

    string index(mapping row) {
	mapping new = ([ ]);
	foreach (readable();; object type) {
	    if (!type->is_index) continue;
	    // TODO: this will only work for single field types
	    // later we somehow have to use filters, Equal by default
	    type->encode_sql(name, row, new);
	}
	return sizeof(new) ? mapping_implode(new, "=", " AND ") : 0;
    }

    string select(mapping row) {
	return index(row);
    }

    string update(mapping row, mapping oldrow) {
	mapping new = ([]);
	foreach (writable();; object type) {
	    if (type == schema->id) continue;
	    type->encode_sql(name, row, new);
	}
	return sizeof(new) ? mapping_implode(new, "=", ", ") : 0;
    }

    mapping insert(mapping row) {
	mapping new = ([]);
	foreach (writable();; object type) {
	    if (type == schema->id) continue;
	    type->encode_sql(name, row, new);
	}
	return sizeof(new) ? new : 0;
    }
}

class Foreign {
    inherit Table;

    string id;
    string fid;

    void create(string name, string id, string fid) {
	this_program::id = id;
	this_program::fid = fid;
	::create(name);
    }

    mapping insert(mapping row) {
	mapping new = ::insert(row);
	if (new) {
	    if (row[id]) {
		new[sprintf("%s.%s", name, fid)]
		    = schema[id]->encode_sql_value(row[id]);
	    } else if (!is_auto_increment) {
		error("join id needs to be either automatic or specified."); 
	    } 
	}
	return new;
    }

    int(0..1) `is_auto_increment() {
	return sql_schema[fid]->flags->auto_increment;
    }
}

class Join {
    inherit Foreign;

    mapping insert(mapping row) {
	mapping new = ::insert(row);
	if (!new) {
	    new = ([]);
	    if (row[id]) {
		new[sprintf("%s.%s", name, fid)]
		    = schema[id]->encode_sql_value(row[id]);
	    } else if (!is_auto_increment) {
		error("join id needs to be either automatic or specified."); 
	    } 
	    // TODO: we somehow have to signal that we want to insert here
	    // anyways.
	}
	return new;
    }

    string update(mapping row, mapping oldrow) {
	string s = ::update(row, oldrow);
	if (s) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		s += sprintf(", %s.%s=%s", name, fid, schema[id]->encode_sql(name, row));
	    } else {
		if (!oldrow[id]) {
		    if (is_auto_increment) {
			mapping new = insert(row);
			// insert data
			// change row[id] to the auto incremented value
			query(sprintf("INSERT INTO %s (%s) VALUES (%s)",  name, indices(row)*",", values(new)*","));
			mapping r = query(sprintf("SELECT %s,version FROM %s WHERE %s=LAST_INSERT_ID();", fid, name, fid))[0];
			row[id] = r[fid];
			return 0;
		    }

		    error("fooobar");
		}
	    }
	}

	return s;
    }

    string join(string table) {
	return sprintf(" INNER JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

// think: country_id where country table is writable by user
class Link {
    inherit Foreign;

    string update(mapping row, mapping oldrow) {
	string s = ::update(row, oldrow);
	if (s) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		if (row[id] != Sql.Null) {
		}
		// check if the new link id has a corresponding field in the other table
		// or Sql.Null
	    }
	}

	return s;
    }

    string join(string table) {
	return sprintf(" JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

// think: country_id where country table is readonly by user
class Reference {
    inherit Link;

    string writable(mapping row, function fun) {
	if (sizeof(row & indices(fields))) {
	    error("Trying to change referenced an hence readonly fields.\n");
	}
    }
}

#if 0
void install_triggers(string table) {
    catch {
	query(sprintf("DROP TRIGGER _syncdb_version_insert_%s;", table));
    };
    catch {
	query(sprintf("DROP TRIGGER _syncdb_version_update_%s;", table));
    };

    query(sprintf(#"CREATE TRIGGER _syncdb_version_insert_%s
	BEFORE INSERT ON %<s
	FOR EACH ROW
	BEGIN
	    DECLARE v INT;
	    SELECT MAX(%<s.version) INTO v FROM %<s WHERE 1;
	    SET NEW.version=v + 1;
	END;
    ;
    ", table));
#ifdef SQL_EVENTS
    query(sprintf(#"CREATE TRIGGER _syncdb_event_update_%s
	AFTER UPDATE ON %<s
	FOR EACH ROW
	BEGIN
	    SELECT * INTO FILE '/dev/shm/interSync/db_%<s' FROM %<s WHERE %<s.version = NEW.version;
	END;
    ", table));
#endif
    query(sprintf(#"CREATE TRIGGER _syncdb_version_update_%s
	BEFORE UPDATE ON %<s
	FOR EACH ROW
	BEGIN
	    DECLARE v INT;
	    SELECT MAX(%<s.version) INTO v FROM %<s WHERE 1;
	    SET NEW.version=v + 1;
	END;
    ", table));
}
#endif


array(Table) table_objects() {
    // TODO: move sorting to create
    array a = values(tables), r;
    sort(indices(tables), a);
    r = filter(a, a->is_auto_increment) + ({ table_o });
    r += filter(a, map(a->is_auto_increment, `!));
    return r;
}

array(string) table_names() {
    return table_objects()->name;
}

mapping tables = ([ ]);
string select_sql, update_sql, insert_sql;

void install_triggers(string table) {
    catch {
	query(sprintf("DROP TRIGGER insert_%s;", table));
    };
    catch {
	query(sprintf("DROP TRIGGER update_%s;", table));
    };

    query(sprintf(#"CREATE TRIGGER insert_%s
	BEFORE INSERT ON %<s
	FOR EACH ROW
	BEGIN
	    DECLARE v INT;
	    SELECT MAX(%<s.version) INTO v FROM %<s WHERE 1;
	    IF v IS NULL THEN
		SET NEW.version=1;
	    ELSE 
		SET NEW.version=v + 1;
	    END IF;
	END;
    ;
    ", table));
    query(sprintf(#"CREATE TRIGGER update_%s
	BEFORE UPDATE ON %<s
	FOR EACH ROW
	BEGIN
	    DECLARE v INT;
	    SELECT MAX(%<s.version) INTO v FROM %<s WHERE 1;
	    IF v IS NULL THEN
		SET NEW.version=1;
	    ELSE 
		SET NEW.version=v + 1;
	    END IF;
	END;
    ", table));
}

void create(string dbname, Sql.Sql con, SyncDB.Schema schema, string table) {
    this_program::table = table;
    sql = con;
    table_o = Table(table);
    ::create(dbname, schema);
    // try to generate all the SQL queries and stored procedures
    //
    // BULLSHIT:
    // 1. determine all tables. if its more than one with complex sum queries,
    //    generate transaction. otherwise if its just one table, we can do with
    //    selects
    array t = ({
    });

#define CASE(x) if (Program.inherits(object_program(type), (x)))


    foreach (schema;; object type) {
	string field = type->name;
	if (type->is_link) {
	    mapping type = type->f_link;
	    foreach (type->tables; string name; string fid) {
		program p;
		CASE(SyncDB.Flags.Join) {
		    p = Join;
		} else CASE(SyncDB.Flags.Reference) {
		    p = Reference;
		} else CASE(SyncDB.Flags.Link) {
		    p = Link;
		} else {
		    error("Unsupported link flag.\n");
		}
		tables[name] = p(name, field, fid);
		if (table_o->sql_schema[field]->flags->auto_increment && tables[name]->is_auto_increment) {
		    error("Link fields cannot be both automatic in %s and %s.\n", table, name);
		}
	    }
	}
    }

    foreach (schema;; object type) {
	if (type->is_foreign) {
	    string t2 = type->f_foreign->table;
	    if (!has_index(tables, t2))
		tables[t2] = Table(t2);
	    tables[t2]->add_field(type);
	} else {
	    table_o->add_field(type);
	}
	t += type->sql_names(table);
    }

    select_sql = sprintf("SELECT %s FROM %s", t*",", table);
    update_sql = sprintf("UPDATE %s SET ", (indices(tables)+({ table }))*",");

    t = ({});
    install_triggers(table);
    update_sql += "%s WHERE ";
    foreach (tables; string foreign_table; Table t) {
	// generate the version triggers
	select_sql += t->join(table);
	update_sql += sprintf("%s.%s = %s.%s AND ", 
		      foreign_table, t->fid, table, t->id);
	// generate proper selects/inserts
	install_triggers(foreign_table);
    }

    update_sql += "%s";
    select_sql += " WHERE 1=1 AND %s";

    // Initialize version
    t = table_names();

    foreach (t; int i; string name) {
	t[i] = sprintf("MAX(%s.version) AS '%<s.version'", name);
    }
    array r = query("SELECT "+t*", "+" FROM %s WHERE 1;", table_names()*",");
    version = schema["version"]->decode_sql(table, r[0]);
}


void select(object filter, function(int(0..1), array(mapping)|mixed:void) cb,
	    mixed ... extra) {
    array(mapping) rows;

    mixed err = catch {
	string index = filter->encode_sql(this);
	if (!sizeof(index)) {
	    // needs error type, i guess
	    cb(1, "Need indexable field(s).\n", @extra);
	    return;
	}
	rows = query(sprintf(select_sql, index));
    };

    if (!err) {
	cb(0, sanitize_result(rows), @extra);
    } else {
	werror("SELECT ERROR: %s\n%s", describe_error(err), describe_backtrace(err[1]));
	cb(1, err, @extra); // convert sql -> atom errors etc.
    }
}

void update(mapping keys, SyncDB.Version version, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    mixed k;
    array|mapping rows;
    string sql = "";
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };
    mapping t = schema->id->encode_sql(table, keys);
    SyncDB.Version oversion, nversion;

    if (!sizeof(t)) {
	cb(1, "Need unique indexable field (or key) to update.\n");
	return;
    }

    string where = mapping_implode(t, "=", " AND ");

    t = schema["version"]->encode_sql(table, ([ "version" : version  ]), t);

    string uwhere = mapping_implode(t, "=", " AND ");


    err = catch {
	query("LOCK TABLES %s WRITE;", table_names() * " WRITE,");
	rows = query(sprintf(select_sql, where))[0];
	oversion = schema["version"]->decode_sql(table, rows);
    };

    sql = filter(table_objects()->update(keys, rows), `!=, 0)*",";

    sql = sprintf(update_sql, sql, uwhere);

    err = catch {
	query(sql);
	if (this_program::sql->master_sql->info) {
	    string info = this_program::sql->master_sql->info();
	    if (!info || -1 != search(info, "Changed: 0")) {
		error("Collision!");
	    }
	}
	rows = query(sprintf(select_sql, where));
	if (sizeof(rows) != 1) error("foo");
	nversion = schema["version"]->decode_sql(table, rows[0]);
	noerr = 1;
    };

    query("UNLOCK TABLES;");

    if (noerr) {
	if (nversion > oversion) {
	    version = nversion;
	    cb(0, sizeof(rows) && sanitize_result(rows[0]));
	} else 
	    cb(1, "Collision!");
    } else {
	cb(1, err);
    }
}

// INSERT INTO table1(c1, c2, c3), table2(c4, c5, c6) VALUES ('v1', 'v2', 
// 'v3',v4, 'v5', 'v6'); 


void insert(mapping row, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    mixed err;
    int(0..1) noerr;
    array rows;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    // TODO:
    // 	schema->key != shcema-»automatic
    if (!schema->automatic) { 
	if (!row[schema->key]) {
	    cb(1, "Could not insert your row, because it misses an indexed & unique field.\n");
	    return;
	}
    } else if (schema->key != schema->automatic) {
	error("RETARDO! (%O != %O)\n", schema->key, schema->automatic);
    }
    err = catch {
	query("LOCK TABLES %s WRITE;", table_names()*" WRITE,");

	// first do the ones which have the fid AUTO_INCREMENT
	// use those automatic values to populate the link ids
	// in the main table
	// insert the main one
	// insert the others

	foreach (table_objects(); ; Table t) {
	    mapping new = t->insert(row);
	    if (!new) continue;
	    string into = indices(new)*",";
	    string values = values(new)*",";
	    query("INSERT INTO %s (%s) VALUES (%s);", t->name, into, values);
	    if (t->is_auto_increment && t->is_link) {
		mapping last = query("SELECT * FROM %s WHERE %s=LAST_INSERT_ID()", t->name, t->fid)[0];
		row[t->id] = (int)last[t->fid];
	    }
	    
	    if (t == table_o && schema[schema->key]->is_automatic) {
		mixed last = query("SELECT LAST_INSERT_ID() as id;");
		werror(">last> %O\n", last);
		if (sizeof(last)) last = last[0];
		werror("setting %s to %s\n", schema->key, last->id);
		row[schema->key] = (int)last->id;
	    }
	}
    };
    if (!err) err = catch {
	string where = mapping_implode(schema->id->encode_sql(table, row), "=",
				       " AND ");
	rows = query(sprintf(select_sql, where));
	if (sizeof(rows) != 1) error("foo");
	version = schema["version"]->decode_sql(table, rows);
	query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sizeof(rows) ? sanitize_result(rows[0]) : 0);
    } else {
	cb(1, err);
    }
}

void syncreq(SyncDB.Version version, mapping filter, function cb, mixed ... args) {
    array(mapping) rows;
    array t = table_objects();
    array(mapping) rows_to_send;
    int cnt;

    werror("SyncDB.MySQL.Table#syncreq(version: %O, filter: %O)\n",
	   version, filter);

    if (sizeof(version) != sizeof(t)) {
	if (!sizeof(version)) {
	    version = SyncDB.Version(allocate(sizeof(t)));
	} else 
	    error("invalid version %O(%d), expected %d entries.", version, 
		  sizeof(version), sizeof(t));
    }

    foreach (t;int i;Table tab) {
	t[i] = sprintf("%s.version > %d", tab->name, version[i]);	
    }

    rows = map(query(sprintf(select_sql, t*" OR ")), sanitize_result);

    if (sizeof(filter)) {
	rows_to_send = allocate(sizeof(rows));

	foreach (rows;; mapping row) {
	    int(0..1) do_send;

	    foreach (filter; string name; object filter) {
		function lookup = filter->has || filter->`[];
		mixed e = catch {
		    werror("syncreq: %O %O %O.\n", row[name], filter, lookup(row[name]));
		    if (lookup(row[name])) {
			do_send = 1;
			continue;
		    }
		};
		if (e) {
		    werror("SyncDB.MySQL.Table#syncreq(...) failed: %O in %O->has(%O(%O)).\n", master()->describe_backtrace(e), filter, row[name], name);
		}
	    }

	    if (do_send) rows_to_send[cnt++] = row;
	}
	rows_to_send = rows_to_send[.. cnt-1];
    } else {
	rows_to_send = rows;
    }

    werror("SyncDB.MySQL.Table#syncreq(...) will send %d rows.\n",
	   sizeof(rows_to_send));

    call_out(cb, 0, 0, rows_to_send, @args, this_program::version);
}

array(mapping)|mapping sanitize_result(array(mapping)|mapping rows) {
    if (mappingp(rows)) {
	mapping new = ([ ]);

	schema->fields->decode_sql(table, rows, new);

	return new;
    } else if (arrayp(rows)) {
	return map(rows, sanitize_result);
    }

}

string get_sql_name(string field) {
    return schema[field]->sql_name(table);
}

/*
LocalTable(name, schema, MeteorTable(name, schema, channel[, db]))
MeteorTable(MysqlTable(schema, dbname, sql))
*/

int main() {
    write("All fine.\n");
    return 0;
}
