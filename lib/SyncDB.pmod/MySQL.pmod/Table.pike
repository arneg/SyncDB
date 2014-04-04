// vim:syntax=lpc
inherit SyncDB.Table;

function(object:void) onupdate;

mapping(string:array) triggers = ([]);

// different types of triggers
// before_insert ( new row )
// after_insert ( new row )
// before_update ( old row, changes )
// after_update ( new row, changes )
// before_delete ( keys )
// after_delete ( keys )

void trigger(string name, mixed ... args) {
    if (triggers[name]) {
        triggers[name](this, @args);
    }
}

void register_trigger(string name, function cb) {
    if (triggers[name]) {
        triggers[name] += ({ cb });
    } else {
        triggers[name] = ({ cb });
    }
}

void unregister_trigger(string name, function cb) {
    array a;
    if (a = triggers[name]) {
        a -= ({ cb });
        if (!sizeof(triggers[name])) {
            m_delete(triggers, name);
        } else {
            triggers[name] = a;
        }
    }
}


function(void:Sql.Sql) _sql_cb;
Sql.Sql _sql;

Sql.Sql `sql() {
    if (_sql_cb) {
        return _sql_cb();
    }

    return _sql;
}

Sql.Sql `sql=(Sql.Sql|function(void:Sql.Sql) o) {
    if (functionp(o)) {
	_sql_cb = o;
    } else {
	_sql = o;
    }
}

string table;
Table table_o;

object sql_error(object sql, mixed err) {
    string state;
    if (!err) return 0;
    // some other error, which we need to pass on
    if (!arrayp(err)) return err;

    if (functionp(sql->sqlstate)) {
        state = sql->sqlstate();
    } else {
        state = "IM001";
    }

    // its not an sql error, afterall
    if (state == "00000") return err;

    return SyncDB.MySQL.Error(state, this, err[0], err[1]);
}

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

private mixed query(mixed ... args) {
    return .Query(@args)(sql);
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

    .Query modified_sql;

    int(0..1) `is_automatic() {
	return schema->id->is_automatic;
    }

    void create(string name) {
	this_program::name = name;
        modified_sql = .Query("SELECT update_time, (NOW() - update_time) > 0 as _time_diff "
                              "FROM information_schema.tables "
                              "WHERE table_schema=DATABASE() AND table_name = %s;", name);
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
	    if (type->is_automatic) continue;
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
		if (row[id] != Val.null) {
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

object restrict(object filter) {
    return .Restriction(this, filter);
}

mapping tables = ([ ]);
.Query select_sql, _update_sql, delete_sql;
object restriction;

.Query update_sql(array(string) fields, array(mixed) values) {
    .Query q = (_update_sql + fields*"=%s, ") + "=%s WHERE ";
    q += values;
    if (restriction) q += restriction->encode_sql(this) + " AND ";
    return q;
}

void install_triggers(string table) {
    string insertt = sprintf(#"BEGIN
            DECLARE v INT;
	    SELECT MAX(ABS(%s.version)) INTO v FROM %<s WHERE 1;
	    IF v IS NULL THEN
		SET NEW.version=1;
	    ELSE 
		SET NEW.version=v + 1;
	    END IF;
	END", table);
    string updatet = sprintf(#"BEGIN
            DECLARE v INT;
            IF NEW.version > 0 THEN
                SELECT MAX(ABS(%s.version)) INTO v FROM %<s WHERE 1;
                IF v IS NULL THEN
                    SET NEW.version=1;
                ELSE 
                    SET NEW.version=v + 1;
                END IF;
            END IF;
	END", table);

    array a = query("SHOW TRIGGERS WHERE `Trigger` = %s;", "insert_"+table);

    if (!sizeof(a) || a[0]->Statement != insertt) {
        if (sizeof(a))
            query(sprintf("DROP TRIGGER insert_%s;", table));
        query(sprintf("CREATE TRIGGER insert_%s BEFORE INSERT ON %<s FOR EACH ROW %s ;", table, insertt));
    }

    a = query("SHOW TRIGGERS WHERE `Trigger` = %s;", "update_"+table);

    if (!sizeof(a) || a[0]->Statement != updatet) {
        if (sizeof(a))
            query(sprintf("DROP TRIGGER update_%s;", table));
        query(sprintf("CREATE TRIGGER update_%s BEFORE UPDATE ON %<s FOR EACH ROW %s ;", table, updatet));
    }
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

string get_unique_identifier(mapping row) {
    function quote = sql->quote;
    // OMFG!!
    return schema->id->Equal(row[schema->key])->encode_sql(this, quote)->render(quote);
}

string table_name() {
    return table;
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

    array table_fields = sql->list_fields(table);

    foreach (schema;; object type) {
        if (type->is_foreign) {
            string t2 = type->f_foreign->table;
            if (!has_index(tables, t2))
                tables[t2] = Table(t2);
            tables[t2]->add_field(type);
        } else {
            table_o->add_field(type);
        }
    }

    if (sizeof(table_fields) != sizeof(table_o->fields))
        t = `+(@table_o->fields->escaped_sql_names(table));

    // TODO: if this is very slow, we should be using a seperate one for limit queries
    select_sql = .Query(sprintf("SELECT SQL_CALC_FOUND_ROWS %s FROM `%s`",
                        sizeof(t) ? t*"," : "*", table));
    _update_sql = .Query(sprintf("UPDATE `%s` SET ", table_names()*"`,`"));
    delete_sql = .Query(sprintf("DELETE FROM `%s` WHERE ", table));

    t = ({});
    install_triggers(table);
    foreach (tables; string foreign_table; Table t) {
	// generate the version triggers
	select_sql += t->join(table);
	// generate proper selects/inserts
	install_triggers(foreign_table);
    }

    select_sql += " WHERE ";

    t = table_names();

    foreach (t; int i; string name) {
        select_sql += sprintf("`%s`.version > 0 AND ", name);
    }

    // Initialize version

    foreach (t; int i; string name) {
	t[i] = sprintf("ABS(MAX(`%s`.version)) AS '%<s.version'", name);
    }

    array r = query(sprintf("SELECT "+t*", "+" FROM `%s` WHERE 1;", table_names()*"`,`"));

    foreach (table_names();; string name) {
        if (!r[0][name+".version"]) r[0][name+".version"] = "0";
    }

    version = schema["version"]->decode_sql(table, r[0]);
}

//! @decl void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)

void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
	    mixed ... extra) {
    select_complex(filter, 0, 0, cb, @extra);
}

//! @decl void select_complex(object filter, object order, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)
//! @expr{order@} is an optional parameter allowing results to be ordered.
void select_complex(object filter, object order, object limit, mixed cb, mixed ... extra) {

    mixed rows;

    object sql = this_program::sql;

    if (restriction) filter &= restriction;

    mixed err = sql_error(sql, catch {
	.Query index = filter->encode_sql(this);

	if (!sizeof(index)) {
	    cb(1, "Need indexable field(s).\n", @extra);
	    return;
	}

	if (order)
            index += order->encode_sql(this);

        if (limit)
            index += limit->encode_sql(this);

	rows = (select_sql + index)(sql);
        if (limit) {
            rows = SyncDB.MySQL.ResultSet(rows);
            rows->num_rows = (int)sql->query("SELECT FOUND_ROWS() as num;")[0]->num;
        }
    });

    if (!err) {
        cb(0, sanitize_result(rows), @extra);
    } else {
	cb(1, err, @extra);
    }
}

void update(mapping keys, mapping|SyncDB.Version version, function(int(0..1),mixed,mixed...:void) cb,
            mixed ... extra) {
    mixed err;
    array|mapping rows;
    object sql = this_program::sql;
    mapping t = ([]);
    SyncDB.Version oversion, nversion;

    object where = get_where(keys);

    schema["version"]->encode_sql(table, ([ "version" : version ]), t);

    object uwhere = where + " AND " + gen_where(t);

    if (restriction) {
        uwhere += " AND " + restriction->encode_sql(this);
    }

    int locked = 0;
    int affected_rows = 0;

    err = sql_error(sql, catch {
	lock_tables(sql);
	locked = 1;
	rows = (select_sql + where)(sql);
	if (sizeof(rows) != 1) error("%O\nCannot find 1 row, found %O", select_sql + where, rows);
	rows = rows[0];
        trigger("before_update", rows, keys);
	oversion = schema["version"]->decode_sql(table, rows);
	mapping new = ([]);
	table_objects()->update(keys, rows, new);
	.Query q = update_sql(indices(new), values(new)) + uwhere;

	q(sql);

        affected_rows = sql->master_sql->affected_rows();

        rows = (select_sql + where)(sql);

        nversion = schema["version"]->decode_sql(table, rows[0]);
    });

    if (locked) unlock_tables(sql);

    if (!err) {
        if (oversion >= nversion) {
            werror("trigger did not increase version. %O >= %O\n%O\n",
                    oversion, nversion,
                    sql->query("SELECT MAX(ABS(version)) FROM "+table_name())
            );
        }
	if (affected_rows == 1 || oversion >= nversion) {
            mapping new = sanitize_result(rows[0]);
            trigger("after_update", new, keys);
            signal_update(nversion, ({ new }));
	    cb(0, new, @extra);
	} else {
	    cb(1, SyncDB.Error.Collision(this, nversion, oversion), @extra);
        }
    } else {
	cb(1, err, @extra);
    }
}

void delete(mapping keys, mapping|SyncDB.Version version, function(int(0..1),mixed,mixed...:void) cb,
            mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    object sql = this_program::sql;

    int locked = 0;
    object where = get_where(keys);
    mapping t = ([]);

    schema["version"]->encode_sql(table, ([ "version" : version ]), t);

    object uwhere = where + " AND " + gen_where(t);

    if (restriction) {
        uwhere += " AND " + restriction->encode_sql(this);
    }

    // if the primary key is not automatic, we do a real delete
    if (schema->automatic) {
        schema["version"]->encode_sql(table, ([ "version" : -version ]), t);

        trigger("before_delete", keys);

        err = sql_error(sql, catch {
            lock_tables(sql);
            locked = 1;
            .Query q = update_sql(indices(t), values(t)) + uwhere;
            q(sql);
            if (sql->master_sql->info) {
                string info = sql->master_sql->info();
                if (!info || -1 != search(info, "Changed: 0")) {
                    error("Collision: %O\n", info);
                }
            }
            noerr = 1;
        });
    } else {
        err = sql_error(sql, catch {
            lock_tables(sql);
            locked = 1;
            .Query q = delete_sql + uwhere;
            q(sql);
            noerr = 1;
        });
    }

    if (locked) unlock_tables(sql);

    if (noerr) {
        trigger("after_delete", keys);
        cb(0, 0, @extra);
        signal_update(version, ({ keys }));
    } else {
	cb(1, err, @extra);
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


void insert(mapping row, function(int(0..1),mixed,mixed...:void) cb, mixed ... extra) {
    mixed err;
    array rows;
    object sql = this_program::sql;

    // TODO:
    // 	schema->key != shcema-»automatic
    if (!schema->automatic) {
	if (!row[schema->key]) {
	    cb(1, "Could not insert your row, because it misses an indexed & unique field.\n",
               @extra);
	    return;
	}
    } else if (schema->key != schema->automatic) {
	error("RETARDO! (%O != %O)\n", schema->key, schema->automatic);
    }

    row = schema->default_row + row;

    trigger("before_inseert", row);

    err = sql_error(sql, catch {

	lock_tables(sql);

	// first do the ones which have the fid AUTO_INCREMENT
	// use those automatic values to populate the link ids
	// in the main table
	// insert the main one
	// insert the others
        //
        // TODO: turn this into _one_ insert into several tables. or else turn this into
        // a transaction
        //
        if (restriction) {
            restriction->insert(row);
        }

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

	.Query where = select_sql + get_where(row);
	rows = where(sql);
	if (sizeof(rows) != 1) {
            if (!sizeof(rows)) {
                error("Trigger on insert not working on table %s\n", table_name());
            } else error("Got more than one row: %O\n", rows);
        }
        rows = sanitize_result(rows);
        row = rows[0];
        signal_update(row->version, rows);
    });

    unlock_tables(sql);

    if (!err) {
        trigger("after_insert", row);
	cb(0, row, @extra);
    } else {
	cb(1, err, @extra);
    }
}

array last_modified;

void request_update(void|function cb, mixed ... args) {
    array t = table_objects();
    array(mapping) rows = ({});
    object sql = this_program::sql;
    array mod = t->modified_sql(sql);

    if (!equal(mod, last_modified)) {
        last_modified = mod;
        .Query _low_update_sql;

        _low_update_sql = select_sql;

        _low_update_sql += "(";

        foreach (t;int i;Table tab) {
            if (i) _low_update_sql += " OR ";
            _low_update_sql += .Query(sprintf("%s.version > %%d", tab->name), version[i]);
        }

        _low_update_sql += ")";

        if (restriction) {
            restriction += " AND " + restriction->encode_sql(this);
        }

        rows = map(_low_update_sql(sql), sanitize_result);

        if (sizeof(rows)) {
            array aa = Array.columns(rows->version->a, enumerate(sizeof(version)));

            foreach (aa; int i; array a) {
                aa[i] = max(@a);
            }

            signal_update(SyncDB.Version(aa), rows);
        }
    }

    if (cb) cb(0, rows, @args);
}

mixed sanitize_result(mixed rows) {
    if (mappingp(rows)) {
	mapping new = ([ ]);

	schema->fields->decode_sql(table, rows, new);

	//return rows->deleted ? SyncDB.DeletedRow(new) : new;;
        return new;
    } else {
        foreach (rows; int i; mapping m) {
            rows[i] = sanitize_result(m);
        }
	return rows;
    }

}

string get_sql_name(string field) {
    return schema[field]->sql_name(table);
}
