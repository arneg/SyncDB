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

mixed query(mixed ... args) {
    string s = sprintf(@args);
    werror("SQL:\t%s\n", s);
    return sql->query(s);
}

class Table {
    object fields = ADT.CritBit.Tree();
    mapping sql_schema = ([]);

    string name;

    void create(string name) {
	this_program::name = name;
	array a = sql->list_fields(name);
	foreach (a;; mapping m) {
	    sql_schema[m->name] = m;
	}
    }

    array(string) field_names() {
	return indices(fields);
    }

    array(string) readable(mapping row, function fun) {
	array(string) t = ({ });
	foreach (fields; string field; object type) {
	    if (!type->is_readable) {
		error("Trying to read non-readable field %s\n", field);
	    }
	    string res = fun(type, field);
	    if (res) t += ({ res });
	}
	return t;
    }

    string index(mapping row) {
	array t = readable(row, lambda(object type, string field) {
	    if (!row[field] || !type->is_index) return 0;
	    return sprintf("%s.%s=%s", name, field, type->encode_sql(row[field]));
	});
	return sizeof(t) ? t*" AND " : 0;
    }

    string select(mapping row) {
	array t = readable(row, lambda(object type, string field) {
	    if (!row[field] || type->is_index) return 0;
	    return sprintf("%s.%s=%s", name, field, type->encode_sql(row[field]));
	});
	return sizeof(t) ? t*" AND " : 0;
    }

    string update(mapping row, mapping oldrow) {
	array t = writable(row, lambda(object type, string field, mixed val) {
	    if (field == schema->key) return 0;
	    return sprintf("%s.%s=%s", name, field, type->encode_sql(val));
	});
	return sizeof(t) ? t*", " : 0;
    }

    array writable(mapping row, function fun) {
	array t = ({});
	foreach (fields;string s; object f) {
	    if (has_index(row, s)) {
		if (f->is_readonly) {
		    error("Trying to modify read-only field %s\n", s);
		}
		string res = fun(f, s, row[s]);
		if (res) t += ({ res });
	    }
	}
	return t;
    }

    string into(mapping row) {
	// check for mandatory fields here.
	array t = writable(row, lambda(object type, string field, mixed val) {
	    return sprintf("%s.%s", name, field);
	});
	return sizeof(t) ? t*", " : 0;
    }


    string values(mapping row) {
	array t = writable(row, lambda(object type, string field, mixed val) {
	    return type->encode_sql(val);
	});
	return sizeof(t) ? t*", " : 0;
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

    string into(mapping row, string|void operation) {
	string s = ::into(row);
	if (s) {
	    if (row[id]) {
		s += sprintf(", %s.%s", name, fid);
	    } else if (!is_auto_increment) {
		error("join id needs to be either automatic or specified."); 
	    } 
	}
	return s;
    }

    string values(mapping row) {
	string s = ::values(row);
	if (s) {
	    if (row[id]) {
		s += ", "+schema[id]->encode_sql(row[id]);
	    } else if (!is_auto_increment) {
		error("join id needs to be either automatic or specified."); 
	    } 
	}
	return s;
    }

    int(0..1) `is_auto_increment() {
	return sql_schema[fid]->flags->auto_increment;
    }
}

class Join {
    inherit Foreign;

    string into(mapping row) {
	string s = ::into(row);
	if (!s) {
	    if (row[id]) return sprintf("%s.%s", name, fid);
	    if (!is_auto_increment)
		error("This field is mandatory.\n");
	    return "";
	}
	return s;
    }

    string values(mapping row) {
	string s = ::values(row);
	if (!s) {
	    if (row[id])
		return schema[id]->encode_sql(row[id]);
	    if (!is_auto_increment)
		error("This field is mandatory.\n");
	    return "";
	}
	return s;
    }

    string update(mapping row, mapping oldrow) {
	string s = ::update(row, oldrow);
	if (s) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		s += sprintf(", %s.%s=%s", name, fid, schema[id]->encode_sql(row[id]));
	    } else {
		if (!oldrow[id]) {
		    if (is_auto_increment) {
			// insert data
			// change row[id] to the auto incremented value
			query(sprintf("INSERT INTO %s (%s) VALUES (%s)",  name, into(row), values(row)));
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


string get_sql_name(string field) {
    object type = schema[field];
    if (type->is_foreign) {
	return sprintf("%s.%s", type->f_foreign->table, type->f_foreign->field||field);
    }
    return sprintf("%s.%s", table, field);
}

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
	table + ".version"
    });

#define CASE(x) if (Program.inherits(object_program(type), (x)))


    foreach (schema->m; string field; object type) {
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
		t += ({ name  + ".version" });
		if (table_o->sql_schema[field]->flags->auto_increment && tables[name]->is_auto_increment) {
		    error("Link fields cannot be both automatic in %s and %s.\n", table, name);
		}
	    }
	}
    }

    foreach (schema->m; string field; object type) {
	if (type->is_foreign) {
	    string t2 = type->f_foreign->table;
	    if (!has_index(tables, t2))
		tables[t2] = Table(t2);
	    tables[t2]->fields[field] = type;
	} else {
	    table_o->fields[field] = type;
	}
	t += ({ sprintf("%s as %s", get_sql_name(field), field) });
    }

    select_sql = sprintf("SELECT %s FROM %s", t*",", table);
    update_sql = sprintf("UPDATE %s,%s SET ", indices(tables)*",", table);

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
    version = SyncDB.Version(sizeof(tables) + 1);
}


void select(mapping keys, function(int(0..1),array(mapping)|mixed:void) cb2, mixed ... extra) {
    int(0..1) noerr;
    array(mapping) rows;
    mixed err;
    array(string) a = allocate(sizeof(keys));
    int i = 0;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };
    
    if (!Array.all(map(map(indices(keys), Function.curry(`[])(schema)), `->, "is_index"), `!=, 0)) {
	werror("!! %O\n", indices(keys));
    }

    err = catch {
	//query("LOCK TABLES %s READ;", table);
	string index = filter(table_objects()->index(keys), `!=, 0) * " AND ";
	if (!sizeof(index)) {
	    cb(1, "Need indexable field(s).\n"); // needs error type, i guess
	    return;
	}
	string t = filter(table_objects()->select(keys), `!=, 0) * " AND ";
	if (sizeof(t)) index += " AND "+t;
	rows = query(sprintf(select_sql, index));
	//query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sanitize_result(rows));
    } else {
	cb(1, err); // convert sql -> atom errors etc.
    }
}

void update(mapping keys, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    mixed k;
    array|mapping rows;
    string sql = "";
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    if (!schema->key && !keys[schema->key]) {
	cb(1, "Need unique indexable field (or key) to update.\n");
	return;
    }

    err = catch {
	query("LOCK TABLES %s WRITE;", table_names() * " WRITE,");
	rows = query(sprintf(select_sql, sprintf("%s=%s", get_sql_name(schema->key), schema[schema->key]->encode_sql(keys[schema->key]))))[0];
    };

    if (keys->version) {
	if (!equal(m_delete(keys, "version"), rows->version)) {
	    query("UNLOCK TABLES;");
	    cb(1, "Version collision.\n");
	    return;
	}
    }

    sql = filter(table_objects()->update(keys, rows), `!=, 0)*",";

    sql = sprintf(update_sql, sql, sprintf("%s=%s", get_sql_name(schema->key), encode(schema->key, keys[schema->key])));

    err = catch {
	query(sql);
	rows = query(sprintf(select_sql, sprintf("%s=%s", get_sql_name(schema->key), schema[schema->key]->encode_sql(keys[schema->key]))));
	noerr = 1;
    };

    query("UNLOCK TABLES;");

    if (noerr) {
	cb(0, sizeof(rows) && sanitize_result(rows[0]));
    } else {
	cb(1, err);
    }
}

// INSERT INTO table1(c1, c2, c3), table2(c4, c5, c6) VALUES ('v1', 'v2', 
// 'v3',v4, 'v5', 'v6'); 


string encode(string field, mixed val) {
    return schema[field]->encode_sql(val);
}

void insert(mapping row, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    mixed err;
    int(0..1) noerr;
    array rows;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    string into = "", values = "";

    // TODO:
    // 	schema->key != shcema-�automatic
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
	// use those automatic values to populare the link ids in the main table
	// insert the main one
	// insert the others

	foreach (table_objects(); ; Table t) {
	    string into = t->into(row);
	    if (!into) continue;
	    string values = t->values(row);
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
	rows = query(sprintf(select_sql, sprintf("%s.%s=%s", table, schema->key, encode(schema->key, row[schema->key]))));
	query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sizeof(rows) ? sanitize_result(rows[0]) : 0);
    } else {
	cb(1, err);
    }
}

void syncreq(SyncDB.Version version, function cb, mixed ... args) {
    array(mapping) rows;
    array t = table_objects();

    if (sizeof(version) != sizeof(t)) error("");

    foreach (t;int i;Table tab) {
	t[i] = sprintf("%s.version > %d", tab->name, version[i]);	
    }

    rows = map(query(sprintf(select_sql, t*" OR ")), sanitize_result);
    call_out(cb, 0, 0, map(rows, sanitize_result), @args, this_program::version);
}

array(mapping)|mapping sanitize_result(array(mapping)|mapping rows) {
    if (mappingp(rows)) {
	mapping new = ([ "version" : SyncDB.Version(sizeof(tables)+1) ]);
	foreach (table_names(); int i; string table) {
	    int v = (int)m_delete(rows, table+".version"); 
	    version[i] = max(v, version[i]);
	    new->version[i] = v; 
	}
	foreach (rows; string field; mixed val) {
	    if (has_value(field, '.')) continue;
	    if (schema[field])
		new[field] = schema[field]->decode_sql(rows[field]);
	    else if (field != "version")
		werror("Field %O unknown to schema: %O\n", field, schema);
	}

	return new;
    } else if (arrayp(rows)) {
	return map(rows, sanitize_result);
    }

}

/*
LocalTable(name, schema, MeteorTable(name, schema, channel[, db]))
MeteorTable(MysqlTable(schema, dbname, sql))
*/

int main() {
    write("All fine.\n");
    return 0;
}
