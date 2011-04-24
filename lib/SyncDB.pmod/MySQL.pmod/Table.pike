constant TABLE = "foo";
inherit SyncDB.Table;

Sql.Sql con;

void create(string dbname, Sql.Sql con, SyncDB.Schema schema) {
    ::create(dbname, schema);
    mapping tables = ([ ]), fields = ([ ]);
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

    foreach (schema->types; string field; object type) {
	if (type->is_key || type->is_index) {
	    queries["get_by_" + field] = sprintf("SELECT %s FROM %s WHERE"
				 " %s=:fetchby: ", field_s, TABLE, field);
	}
    }
#endif

    this_program::con = con;
}


void select(mapping keys, function(int(0..1),array(mapping)|mixed:void) cb2, mixed ... extra) {
    string sql = sprintf("SELECT * FROM %s WHERE 1=1", TABLE);
    int(0..1) noerr;
    array(mapping) rows;
    mixed err;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };
    
    if (!Array.all(map(map(indices(keys), Function.curry(`[])(schema)), `->, "is_index"), `!=, 0)) {
	werror("!! %O\n", indices(keys));
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

void update(mapping keys, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    mixed k;
    array rows;
    string sql = sprintf("UPDATE %s SET ", TABLE);
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    if (!schema->key && !keys[schema->key]) {
	cb(1, "Need unique indexable field (or key) to update.\n");
	return;
    }

    foreach (keys; string field; mixed val) {
	if (field != schema->key)
	    sql = sprintf("%s %s=%s", sql, field, schema[field]->encode_sql(val));
    }
    
    sql += sprintf(" WHERE %s=%s;", schema->key, schema[schema->key]->encode_sql(keys[schema->key]));

    err = catch {
	con->query(sprintf("LOCK TABLES %s WRITE;", TABLE));
	con->query(sql);
	rows = con->query(sprintf("SELECT * FROM %s WHERE %s=%s;", TABLE, schema->key, schema[schema->key]->encode_sql(keys[schema->key])));
	con->query("UNLOCK TABLES;");
	noerr = 1;
    };

    if (noerr) {
	cb(0, sizeof(rows) && sanitize_result(rows[0]));
    } else {
	cb(1, err);
    }
}

void insert(mapping row, function(int(0..1),mapping|mixed:void) cb2, mixed ... extra) {
    mixed err;
    int(0..1) noerr;
    array keys = allocate(sizeof(row)), vals = allocate(sizeof(row)), rows;
    int cnt;
    mixed cb(int(0..1) error, mixed bla) {
	return cb2(error, bla, @extra);
    };

    foreach (row; string field; mixed val) {
	keys[cnt] = field;
	werror(">> %O %O %O\n", schema->m, field, schema[field]);
	vals[cnt] = schema[field]->encode_sql(val);
    }

    // TODO:
    // 	schema->key != shcema-»automatic
    if (!schema->automatic) { 
	if (!row[schema->key]) {
	    cb(1, "Could not insert your row, because it misses an indexed & unique field.\n");
	    return;
	}
	err = catch {
	    con->query(sprintf("LOCK TABLES %s WRITE;", TABLE));
	    con->query(sprintf("INSERT INTO %s (%s) VALUES(%s);", TABLE, keys * ",", vals * ","));
	    rows = con->query(sprintf("SELECT * FROM %s WHERE %s=%s;", TABLE, schema->key, row[schema->key]));
	    con->query("UNLOCK TABLES;");
	    noerr = 1;
	} ;
    } else if (schema->key != schema->automatic) {

	error("RETARDO! (%O != %O)\n", schema->key, schema->automatic);
    } else {
	err = catch {
	    con->query(sprintf("LOCK TABLES %s WRITE;", TABLE));
	    con->query(sprintf("INSERT INTO %s (%s) VALUES(%s);", TABLE, keys * ",", vals * ","));
	    rows = con->query(sprintf("SELECT * FROM %s WHERE %s=LAST_INSERT_ID();", TABLE, schema->automatic));
	    con->query("UNLOCK TABLES;");
	    noerr = 1;
	} ;
    }


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
	    new[field] = schema[field]->decode_sql(rows[field]);
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
