string dbname;
Sql.Sql con;
SyncDB.Schema schema;

void create(string dbname, Sql.Sql con, SyncDB.Schema schema) {
    mapping tables = ([ ]), fields = ([ ]);
    this_program::dbname = dbname;
    this_program::con = con;
    this_program::schema = schema;
    // try to generate all the SQL queries and stored procedures
    //
    // 1. determine all tables. if its more than one with complex sum queries,
    //    generate transaction. otherwise if its just one table, we can do with
    //    selects
#if 0
    foreach (schema->types; string field, object type) {
	if (type->is_foreign) {
	    if (!has_index(tables[type->table])) tables[type->table] = ({ });
	    tables[type->table] += ({ field });
	}
    }
#endif

    foreach (schema->types; string field; object type) {
	if (type->is_key || type->is_index) {
	    queries["get_by_" + field] = sprintf("SELECT %s FROM %s WHERE"
				 " %s=:fetchby: ", field_s, TABLE, field);
	}
    }
}


void get(mapping keys, function(int(0..1),array(mapping)|mixed:void) cb) {
    string sql = sprintf("SELECT * FROM %s WHERE 1=1", TABLE);
    int(0..1) noerr;
    array(mapping) rows;
    mixed err;
    
    if (!Array.all(map(map(map(indices(keys), Function.curry(`->)(schema)), `->, "flags"), `->, "indexable"), Array.any, !=, 0)) {
	cb(1, "Need indexable field(s).\n"); // needs error type, i guess
	return;
    }

    foreach (keys; string field; mixed val) {
	err = catch {
	    sql = sprintf("%s AND %s=%s", sql, field, schema[field]->encode_sql(val));
	    noerr = 1;
	};

	if (noerr) {
	    noerr = 0;
	} else {
	    cb(1, err);
	    return;
	}
    }

    sql += ";";

    err = catch {
	con->query(sprintf("LOCK TABLES %s READ;", TABLE));
	rows = con->query(sql);
	con->query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sanitize_result(rows));
    } else {
	cb(1, err); // convert sql -> atom errors etc.
    }
}

void set(mapping keys, function(int(0..1),mapping|mixed:void) cb) {
    int(0..1) noerr;
    mixed err;
    mixed k;
    array rows;
    string sql = sprintf("UPDATE %s SET ", TABLE);

    {
	mixed u;
	foreach (keys; string field;) {
	    if (Array.any(schema[field]->flags->unique, `!=, 0)
		&& Array.any(schema[field]->flags->indexable, `!=, 0)) {
		u = field;
	    }
	    if (Array.any(schema[field]->flags->key, !=, 0)) {
		k = field;
	    }
	}

	if (!k) k = u;
    }

    if (!k) {
	cb(1, "Need unique indexable field (or key) to set.\n");
	return;
    }

    foreach (keys; string field; mixed val) {
	sql = sprintf("%s %s=%s", sql, field, schema[field]->encode_sql(val));
    }
    sql += ";";

    err = catch {
	con->query(sprintf("LOCK TABLES %s WRITE;", TABLE));
	con->query(sql);
	rows = con->query(sprintf("SELECT * FROM %s WHERE %s=%s;", TABLE, k, schema[k]->encode_sql(key[k])));
	con->query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sizeof(rows) && sanitize_result(rows[0]));
    } else {
	cb(1, err);
    }
}

void add(mapping row, function(int(0..1),mapping|mixed:void) cb) {
    mixed err;
    int(0..1) noerr;
    array keys = allocate(sizeof(row)), values = allocate(sizeof(row)), rows;
    int cnt;
    string k;

    {
	string u;

	foreach (keys; string field; mixed val) {
	    keys[cnt] = field;
	    vals[cnt] = schema[field]->encode_sql(val);
	    if (Array.any(schema[field]->flags->index, `!=, 0)
		&& Array.any(schema[field]->flags->unique, `!=, 0)) {
		u = field;
	    }
	    if (Array.any(schema[field]->flags->key, `!=, 0)) {
		k = field;
	    }
	}
	if (!k) k = u;
    }

    err = catch {
	con->query(sprintf("LOCK TABLES %s WRITE;", TABLE));
	con->query(sprintf("INSERT INTO %s (%s) VALUES(%s);", TABLE, keys * ",", vals * ","));
	rows = con->query(sprintf("SELECT * FROM %s WHERE %s=%s;", TABLE, k, row[k]));
	con->query("UNLOCK TABLES;");
	noerr = 1;
    } ;

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
	    new[field] = rows[field];
	}

	return new;
    } else if (arrayp(rows)) {
	return map(rows, sanitize_result);
    }

}

LocalTable(name, schema, MeteorTable(name, schema, channel[, db]))
MeteorTable(MysqlTable(schema, dbname, sql))
