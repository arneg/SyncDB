// vim:syntax=lpc
inherit SyncDB.Table;

Sql.Sql sql;
string table;
Table table_o;

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

class Table(string name) {
    object fields = ADT.CritBit.Tree();

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
		SET NEW.version=v + 1;
	    END;
	;
	", table));
	query(sprintf(#"CREATE TRIGGER update_%s
	    BEFORE UPDATE ON %<s
	    FOR EACH ROW
	    BEGIN
		DECLARE v INT;
		SELECT MAX(%<s.version) INTO v FROM %<s WHERE 1;
		SET NEW.version=v + 1;
	    END;
	", table));
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

    string update(mapping row) {
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

class Foreign(string name) {
    inherit Table;
    string id;
    string fid;

}

class Join(string name) {
    inherit Foreign;

    string update(mapping row) {
	string s = ::update(row);
	if (s) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		s += sprintf(", %s.%s=%s", name, fid, schema[id]->encode_sql(row[id]));
	    }
	}

	return s;
    }

    string join(string table) {
	return sprintf(" INNER JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

class Link(string name) {
    inherit Foreign;

    string join(string table) {
	return sprintf(" JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

class Reference(string name) {
    inherit Link;

    string writable(mapping row, function fun) {
	if (sizeof(row & indices(fields))) {
	    error("Trying to change referenced an hence readonly fields.\n");
	}
    }
}

string get_sql_name(string field) {
    object type = schema[field];
    if (type->is_foreign) {
	return sprintf("%s.%s", type->f_foreign->table, type->f_foreign->field||field);
    }
    return sprintf("%s.%s", table, field);
}

array(string) table_names() {
    return indices(tables) + ({ table });
}

array(Table) table_objects() {
    return values(tables) + ({ table_o });
}

mapping tables = ([ ]);
string select_sql, update_sql, insert_sql;


void create(string dbname, Sql.Sql con, SyncDB.Schema schema, string table) {
    this_program::table = table;
    table_o = Table(table);
    sql = con;
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
		CASE(SyncDB.Flags.Join) {
		    tables[name] = Join(name);
		} else CASE(SyncDB.Flags.Reference) {
		    tables[name] = Reference(name);
		} else CASE(SyncDB.Flags.Link) {
		    tables[name] = Link(name);
		} else {
		    error("Unsupported link flag.\n");
		}
		tables[name]->id = field;
		tables[name]->fid = fid;
		t += ({ name  + ".version" });
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
	rows = query(sprintf(select_sql, t));
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
    array rows;
    string sql = "";
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    if (!schema->key && !keys[schema->key]) {
	cb(1, "Need unique indexable field (or key) to update.\n");
	return;
    }

    sql = filter(table_objects()->update(keys), `!=, 0)*",";
    
    sql = sprintf(update_sql, sql, sprintf("%s=%s", get_sql_name(schema->key), encode(schema->key, keys[schema->key])));

    err = catch {
	query("LOCK TABLES %s WRITE;", table_names() * " WRITE,");
	query(sql);
	rows = query(sprintf(select_sql, sprintf("%s=%s", get_sql_name(schema->key), schema[schema->key]->encode_sql(keys[schema->key]))));
	query("UNLOCK TABLES;");
	noerr = 1;
    };

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
    array keys = allocate(sizeof(row)), vals = allocate(sizeof(row)), rows;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    string into = "", values = "";

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

	foreach (table_objects(); ; Table t) {
	    string into = t->into(row);
	    if (!into) continue;
	    string values = t->values(row);
	    query("INSERT INTO %s (%s) VALUES (%s);", t->name, into, values);
	}
	// use select on the ip here.
    };
    if (!err) err = catch {
	if (!schema->automatic) {
	    rows = query(sprintf(select_sql, sprintf("%s=%s", schema->key, encode(schema->key, row[schema->key]))));

	} else {
	    rows = query(sprintf(select_sql, sprintf("%s=LAST_INSERT_ID()", get_sql_name(schema->automatic))));
	}
	query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sizeof(rows) ? sanitize_result(rows[0]) : 0);
    } else {
	cb(1, err);
    }
}

array(mapping)|mapping sanitize_result(array(mapping)|mapping rows) {
    if (mappingp(rows)) {
	mapping new = ([ "version" : ({}) ]);
	foreach (table_names();; string table) {
	    new->version += ({ (int)m_delete(rows, table+".version") }); 
	}
	foreach (rows; string field; mixed val) {
	    if (has_value(field, '.')) continue;
	    if (schema[field])
		new[field] = schema[field]->decode_sql(rows[field]);
	    else
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
