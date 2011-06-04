inherit SyncDB.Table;

Sql.Sql sql;
string table;
Table table_o;

mixed query(mixed ... args) {
    string s = sprintf(@args);
    werror("SQL:\t%s\n", s);
    return sql->query(s);
}

class Table(string name) {
    array(string) fields = ({});
    string id;
    string fid;
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
    array t = ({});

    foreach (schema->m; string field; object type) {
	if (type->is_foreign) {
	    string t2 = type->f_foreign->table;
	    if (!has_index(tables, t2))
		tables[t2] = Table(t2);
	    tables[t2]->fields += ({ field });
	} else {
	    table_o->fields += ({ field });
	    if (type->is_join) {
		mapping t = type->f_join->tables;
		foreach (t; string name; string fid) {
		    if (!has_index(tables, name))
			tables[name] = Table(name);
		    tables[name]->id = field;
		    tables[name]->fid = fid;
		}
	    }
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
	select_sql +=
	      sprintf(" INNER JOIN %s ON %s.%s=%s.%s", foreign_table,
		      foreign_table, t->fid, table, t->id);
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
	cb(1, "Need indexable field(s).\n"); // needs error type, i guess
	return;
    }

    foreach (keys; string field; mixed val) {
	if (sizeof(tables)) {
	    err = catch {
		a[i++] = sprintf("%s=%s", get_sql_name(field), encode(field, val));
		noerr = 1;
	    };
	}

	if (noerr) {
	    noerr = 0;
	} else {
	    cb(1, err);
	    return;
	}
    }

    err = catch {
	//query("LOCK TABLES %s READ;", table);
	rows = query(sprintf(select_sql, a*" AND "));
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

    foreach (keys; string field; mixed val) {
	if (field != schema->key)
	    sql = sprintf("%s %s=%s,", sql, get_sql_name(field), schema[field]->encode_sql(val));
    }
    sql = sql[..sizeof(sql)-2];
    
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
	    string into = "";
	    string values = "";
	    array a = filter(t->fields, Function.curry(`->)(row));
	    if (sizeof(a)) {
		if (!row[t->id]) {
		    if (schema[t->id]->is_automatic) {
			// TODO: row[t->id] = 
		    } else 
			error("field join JOIN not specified.\n");
		}
		foreach (a;; string field) {
		    values += encode(field, row[field]) + ",";
		    into += sprintf("%s,", field);
		}
	    }
	    into = into[..sizeof(into)-2];
	    values = values[..sizeof(values)-2];
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
	mapping new = ([ ]);
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
