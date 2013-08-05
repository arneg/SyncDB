// vim:syntax=lpc
inherit SyncDB.Table;

function(void:Sql.Sql) _sql_cb;
Sql.Sql _sql;

Sql.Sql `sql() {
    if (_sql_cb && (!_sql || !_sql->is_open() || _sql->ping() == -1)) {
	_sql = _sql_cb();
    }

    return _sql;
}

Sql.Sql `sql=(Sql.Sql|function(void:Sql.Sql) o) {
    if (functionp(o)) {
	_sql_cb = o;
	_sql = o();
    } else {
	_sql = o;
    }
}

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
private mixed query(mixed ... args) {
    string s = sprintf(@args);
#ifdef DB_DEBUG
    werror("SQL:\t%s\n", String.width(s) > 8 ? string_to_utf8(s) : s);
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

    int(0..1) `is_automatic() {
	return schema->id->is_automatic;
    }

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

    void update(mapping row, mapping oldrow, mapping new) {
	foreach (writable();; object type) {
	    if (type == schema->id) continue;
	    type->encode_sql(name, row, new);
	}
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
	    } else if (!is_automatic) {
		error("join id needs to be either automatic or specified."); 
	    } 
	}
	return new;
    }

    int(0..1) `is_automatic() {
	werror("auto_increment: %O %O %O\n", sql_schema[fid], sql_schema[fid]->flags, sql_schema[fid]->flags->auto_increment);
	return sql_schema[fid]->is_automatic;
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
	    } else if (!is_automatic) {
		error("join id needs to be either automatic or specified."); 
	    } 
	    // TODO: we somehow have to signal that we want to insert here
	    // anyways.
	}
	return new;
    }

    void update(mapping row, mapping oldrow, mapping new) {
	int i = sizeof(new);
	::update(row, oldrow, new);
	if (sizeof(new) - i) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		schema[id]->encode_sql(name, row, new);
	    } else {
		if (!oldrow[id]) {
		    if (is_automatic) {
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
    }

    string join(string table) {
	return sprintf(" INNER JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

// think: country_id where country table is writable by user
class Link {
    inherit Foreign;

    void update(mapping row, mapping oldrow, mapping new) {
	int i = sizeof(new);
	::update(row, oldrow, new);
	if (sizeof(new) - i) { // need to add the corresponding link id
	    if (has_index(row, id)) {
		if (row[id] != Sql.Null) {
		}
		// check if the new link id has a corresponding field in the other table
		// or Sql.Null
	    }
	}
    }

    string join(string table) {
	return sprintf(" JOIN %s ON %s.%s=%s.%s", name,
		      name, fid, table, id);
    }
}

// think: country_id where country table is readonly by user
class Reference {
    inherit Link;

    array(string) writable() {
	return ({});
    }

    string insert(mapping row, function fun) {
	if (sizeof(row & fields->name)) {
	    error("Trying to change referenced an hence readonly fields: %O.\n", row & fields->name);
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
    r = filter(a, a->is_automatic) + ({ table_o });
    r += filter(a, map(a->is_automatic, `!));
    return r;
}

array(string) table_names() {
    return table_objects()->name;
}

mapping tables = ([ ]);
.Query select_sql, _update_sql, restriction;

.Query update_sql(array(string) fields, array(mixed) values) {
    .Query q = (_update_sql + fields*"=%s, ") + "=%s WHERE ";
    q += values;
    if (restriction) q += restriction + " AND ";
    return q;
}

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

object gen_where(mapping t) {
    array a = allocate(sizeof(t));
    array values = allocate(sizeof(t));
    int i = 0;

    foreach (t; string field; mixed v) {
	a[i] = sprintf("%s = %%s", field);
	values[i++] = v;
    }

    return .Query(a * " AND ", @values);
}

object get_where(mapping keys) {
    mapping t = schema->id->encode_sql(table, keys);
    return gen_where(t);
}

void create(string dbname, Sql.Sql|function(void:Sql.Sql) con, SyncDB.Schema schema, string table) {
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
		if (table_o->sql_schema[field]->flags->is_automatic && tables[name]->is_automatic && schema[fid]->is_writable) {
		    error("Link fields cannot be both automatic in %s and %s (%O, %O).\n", table, name, fid, schema[fid]->flags);
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

    select_sql = .Query(sprintf("SELECT %s FROM %s", t*",", table));
    _update_sql = .Query(sprintf("UPDATE %s SET ", table_names()*","));

    t = ({});
    install_triggers(table);
    foreach (tables; string foreign_table; Table t) {
	// generate the version triggers
	select_sql += t->join(table);
	/*
	update_sql += sprintf("%s.%s = %s.%s AND ", 
		      foreign_table, t->fid, table, t->id);
	*/
	// generate proper selects/inserts
	install_triggers(foreign_table);
    }

    select_sql += " WHERE 1=1 AND ";
    if (schema->restriction) {
	restriction = schema->restriction->encode_sql(this);

	select_sql += restriction;
	select_sql += " AND ";
    }
    // Initialize version
    t = table_names();

    foreach (t; int i; string name) {
	t[i] = sprintf("MAX(%s.version) AS '%<s.version'", name);
    }
    array r = query("SELECT "+t*", "+" FROM %s WHERE 1;", table_names()*",");
    version = schema["version"]->decode_sql(table, r[0]);
}

//! @decl void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)
//! @decl void select(object filter, object order, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)
//! @expr{order@} is an optional parameter allowing results to be ordered.

void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
	    mixed ... extra) {
    array(mapping) rows;
    object order;

    if (objectp(cb)) {
	order = cb;
	cb = extra[0];
	extra = extra[1..];
    }

    mixed err = catch {
	//werror(">>>>\t%O\n", select_sql);
	mixed foo = filter->encode_sql(this);
	//werror("foo : %O\n", foo);
	.Query index = filter->encode_sql(this);
	//werror("<<<<\t%O\n", index);
	if (!sizeof(index)) {
	    // needs error type, i guess
	    cb(1, "Need indexable field(s).\n", @extra);
	    return;
	}
	if (order)
	    index += " ORDER BY " + order->encode_sql(this);
	rows = (select_sql + index)(sql);
    };

    if (!err) {
	cb(0, sanitize_result(rows), @extra);
    } else {
	werror("SELECT ERROR: %s\n%s", describe_error(err), describe_backtrace(err[1]));
	cb(1, err, @extra); // convert sql -> atom errors etc.
    }
}

void update(mapping keys, mapping|SyncDB.Version version, function(int(0..1),mixed,mixed...:void) cb2, mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    array|mapping rows;
    string sql_query = "";
    object sql = this_program::sql;
    mapping t = ([]);
    void cb(int(0..1) error, mixed bla) {
	cb2(error, bla, @extra);
	return;
    };
    SyncDB.Version oversion, nversion;
    object where = get_where(keys);

    if (!sizeof(where)) {
	cb(1, "Need unique indexable field (or key) to update.\n");
	return;
    }

    if (!mappingp(version)) version = ([ "version" : version ]);
    foreach (version; string name; mixed value) {
	schema[name]->encode_sql(table, version, t);
    }

    object uwhere = where + " AND " + gen_where(t);
    werror("-> %O\n", uwhere);

    int locked = 0;

    err = catch {
	lock_tables(sql);
	locked = 1;
	rows = (select_sql + where)(sql);
	if (sizeof(rows) != 1) error("foo");
	rows = rows[0];
	oversion = schema["version"]->decode_sql(table, rows);
	mapping new = ([]);
	table_objects()->update(keys, rows, new);
	.Query q = update_sql(indices(new), values(new)) + uwhere;
	werror("UPDATE: %O\n", q);
	q(sql);
	if (sql->master_sql->info) {
	    string info = sql->master_sql->info();
	    if (!info || -1 != search(info, "Changed: 0")) {
		error("Collision: %O\n", info);
	    }
	}
	rows = (select_sql + where)(sql);
	if (sizeof(rows) != 1) error("foo");
	nversion = schema["version"]->decode_sql(table, rows[0]);
	noerr = 1;
    };

    if (locked) unlock_tables(sql);

    if (noerr) {
	if (nversion > oversion) {
	    version = nversion;
	    cb(0, sizeof(rows) && sanitize_result(rows[0]));
	} else 
	    cb(1, sprintf("Collision! old version: %O vs new version: %O\n", oversion, nversion));
    } else {
	cb(1, err);
    }
}

// INSERT INTO table1(c1, c2, c3), table2(c4, c5, c6) VALUES ('v1', 'v2', 
// 'v3',v4, 'v5', 'v6'); 
//

.Query `lock_tables() {
    return .Query(sprintf("LOCK TABLES %s WRITE;", table_names()*" WRITE,"));
}

.Query `unlock_tables() {
    return .Query("UNLOCK TABLES;");
}


void insert(mapping row, function(int(0..1),mixed,mixed...:void) cb2, mixed ... extra) {
    mixed err;
    int(0..1) noerr;
    array rows;
    void cb(int(0..1) error, mixed bla) {
	cb2(error, bla, @extra);
	return;
    };
    object sql = this_program::sql;

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
	row = schema->default_row + row;
	lock_tables(sql);

	// first do the ones which have the fid AUTO_INCREMENT
	// use those automatic values to populate the link ids
	// in the main table
	// insert the main one
	// insert the others

	foreach (table_objects(); ; Table t) {
	    mapping new = t->insert(row);
	    if (!new) {
		if (t->is_automatic && sizeof(t->writable())) {
		    new = ([]);
		    // DEAD
		}
		continue;
	    }
	    string into = indices(new)*",";
	    object insert_sql = .Query("INSERT INTO " + t->name + " (" + indices(new)*"," + ") VALUES ("
				       + allocate(sizeof(new), "%s")*"," + ");", @values(new));

	    insert_sql(sql);
	    // we need todo this potentially for all automatic fields (not only
	    // mysql auto increment).
	    if (t->is_automatic && t->is_ass_on_fire && t->is_link) {
		mapping last = sql->query("SELECT * FROM %s WHERE %s=LAST_INSERT_ID()", t->name, t->fid)[0];
		row[t->id] = (int)last[t->fid];
	    }
	    
	    if (t == table_o && schema[schema->key]->is_automatic) {
		mixed last = sql->query("SELECT LAST_INSERT_ID() as id;");
		if (sizeof(last)) last = last[0];
		row[schema->key] = (int)last->id;
	    }
	}
    };
    if (!err) err = catch {
	.Query where = select_sql + get_where(row);
	rows = where(sql);
	if (sizeof(rows) != 1) error("foo");
	version = schema["version"]->decode_sql(table, rows[0]);
	noerr = 1;
    };
    unlock_tables(sql);

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

    rows = map((select_sql + t*" OR ")(sql), sanitize_result);

    if (sizeof(filter)) {
	rows_to_send = allocate(sizeof(rows));

	foreach (rows;; mapping row) {
	    int(0..1) do_send;

OUTER: 	    foreach (filter; string name; object filter) {
		function lookup = filter->has || filter->`[];
		mixed e = catch {
		    werror("syncreq: %O %O %O.\n", row[name], filter, lookup(row[name]));
		    int val = lookup(row[name]);
		    switch (val) {
		    case 1:
			do_send = 1;
			//continue OUTER;
			break;
		    case -1:
			do_send = 0;
			break OUTER;
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

	return rows->deleted ? SyncDB.DeletedRow(new) : new;;
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
