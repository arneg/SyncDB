// vim:syntax=lpc
inherit SyncDB.Table;

function(void:Sql.Sql) _sql_cb;
Sql.Sql _sql;

Sql.Sql `sql() {
    if (_sql_cb) {
        Sql.Sql o = _sql_cb();
        if (o->get_charset() != "unicode") error("Expect Sql.Sql objects with charset 'unicode'.\n");
        return o;
    }

    return _sql;
}

Sql.Sql `sql=(Sql.Sql|function(void:Sql.Sql) o) {
    if (functionp(o)) {
	_sql_cb = o;
    } else {
        if (o->get_charset() != "unicode") error("Expect Sql.Sql objects with charset 'unicode'.\n");
	_sql = o;
    }
}

private string table;
private Table table_o;

object sql_error(object sql, mixed err) {
    string state;
    if (!err) return 0;
    // some other error, which we need to pass on
    if (!arrayp(err)) return err;
    if (!sql) return err;

    if (functionp(sql->sqlstate)) {
        state = sql->sqlstate();
    } else {
        state = "IM001";
    }

    // its not an sql error, afterall
    if (state == "00000") return err;

    return SyncDB.MySQL.Error(state, this, err[0], err[1]);
}

private class Table {
    array(SyncDB.Types.Base) fields = ({});
    mapping sql_schema = ([]);

    string name;

    .Query modified_sql;

    int(0..1) `is_automatic() {
	return schema->id->is->automatic;
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
        foreach (type->sql_names();; string column) {
            if (!sql_schema[column]) error("Unknown column %O\n", column);
        }
	fields += ({ type });
    }

    array(string) sql_names() {
	return predef::`+(@fields->sql_names(name));
    }

    array(string) escaped_sql_names() {
        if (this == table_o)
            return predef::`+(@fields->escaped_sql_names(0));
        else
            return predef::`+(@fields->escaped_sql_names(name));
    }

    array(SyncDB.Types.Base) writable() {
	return filter(fields, fields->is->writable);
    }

    void update(mapping row, mapping oldrow, mapping new) {
        string table_name = this == table_o ? 0 : name;
	foreach (writable();; object type) {
	    if (type == schema->id) continue;
	    type->encode_sql(table_name, row, new);
	}
    }

    mapping insert(mapping row) {
        string table_name = this == table_o ? 0 : name;
	mapping new = ([]);
	foreach (writable();; object type) {
	    type->encode_sql(table_name, row, new);
	}
	return new;
    }
}

object restrict(object filter) {
    return .Restriction(this, filter);
}

.Query select_sql, select_sql_count, _update_sql, delete_sql, count_sql;

.Query update_sql(array(string) fields, array(mixed) values) {
    .Query q = (_update_sql + fields*"=%s, ") + "=%s WHERE ";
    q += values;
    return q;
}

void install_triggers(string table) {
    Sql.Sql sql = this_program::sql;

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

    lock_tables(sql);

    array a = sql->query("SHOW TRIGGERS WHERE `Table` = %s AND `Event` = 'INSERT';", table);

    if (!sizeof(a) || a[0]->Statement != insertt) {
        if (sizeof(a))
            sql->query(sprintf("DROP TRIGGER %s;", a[0]->Trigger));
        sql->query(sprintf("CREATE TRIGGER insert_%s BEFORE INSERT ON %<s FOR EACH ROW %s ;", table, insertt));
    }

    a = sql->query("SHOW TRIGGERS WHERE `Table` = %s AND `Event` = 'UPDATE';", table);

    if (!sizeof(a) || a[0]->Statement != updatet) {
        if (sizeof(a))
            sql->query(sprintf("DROP TRIGGER %s;", a[0]->Trigger));
        sql->query(sprintf("CREATE TRIGGER update_%s BEFORE UPDATE ON %<s FOR EACH ROW %s ;", table, updatet));
    }

    unlock_tables(sql);
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
    return row[schema->key];
}

string table_name() {
    return table;
}

void create(string dbname, Sql.Sql|function(void:Sql.Sql) con, SyncDB.Schema schema, void|string table) {
    if (!table) table = dbname;
    this_program::table = table;
    sql = con;
    table_o = Table(table);
    ::create(dbname, schema);
    //
    array t = ({
    });

    array table_fields = sql->list_fields(table);

    foreach (schema;; object type) {
        table_o->add_field(type);
    }

    if (sizeof(table_fields) != sizeof(table_o->sql_names()))
        t = table_o->escaped_sql_names();

    select_sql_count = .Query(sprintf("SELECT SQL_CALC_FOUND_ROWS %s FROM `%s`",
                                      sizeof(t) ? t*"," : "*", table));
    select_sql = .Query(sprintf("SELECT %s FROM `%s`", sizeof(t) ? t*"," : "*", table));
    _update_sql = .Query(sprintf("UPDATE `%s` SET ", table_name()));
    delete_sql = .Query(sprintf("DELETE FROM `%s` WHERE ", table));

    count_sql = .Query(sprintf("SELECT COUNT(*) as cnt from `%s` WHERE ", table));

    install_triggers(table);

    select_sql += " WHERE ";
    select_sql_count += " WHERE ";

    string vf = sprintf("`%s`.version > 0 AND ", table_name());
    count_sql += vf;
    select_sql += vf;
    select_sql_count += vf;

    // Initialize version
    update_table_version();
}

void update_table_version(void|object con) {
    if (!con) con = sql;

    array r = con->query(sprintf("SELECT ABS(MAX(version)) AS version FROM `%s`;", table_name()));

    if (!sizeof(r)) {
        version = 0;
    } else version = (int)r[0]->version;
}

//! @decl void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)

void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb,
	    mixed ... extra) {
    select_complex(filter, 0, 0, cb, @extra);
}

object|array(mapping) low_select_complex(object filter, object order, object limit) {
    object sql = this_program::sql;

    mixed err = sql_error(sql, catch {
        object|array(mapping) rows;
	.Query index;
        
        if (filter) index = filter->encode_sql(this);
        else index = .Query("TRUE");

	if (order)
            index += order->encode_sql(this);

        if (limit) {
            index += limit->encode_sql(this);

            rows = (select_sql_count + index)(sql);
        } else {
            rows = (select_sql + index)(sql);
        }

        rows = sanitize_result(rows);

        if (limit) {
            rows = SyncDB.MySQL.ResultSet(rows);
            rows->num_rows = (int)sql->query("SELECT FOUND_ROWS() as num;")[0]->num;
        }

        return rows;
    });

    throw(err);
}

class PageIterator {
    inherit Iterator;

    object filter, order;
    int rows, page = 0;

    object data;

    void create(object filter, object order, int rows) {
        this_program::filter = filter;
        if (!order) {
            if (schema->id) {
                order = .Select.OrderBy(.Select.ASC(schema->id));
            } else if (schema->version) {
                order = .Select.OrderBy(.Select.ASC(schema->version));
            } else {
                werror("Warning: no reliable ordering in PageIterator.\n");
            }
        }
        this_program::order = order;
        this_program::rows = rows;
    }

    private void fetch() {
        data = low_select_complex(filter, order, .Select.Limit(page * rows, rows));
    }

    int index() {
        return page;
    }

    int first() {
        page = 0;
        return !!this;
    }

    mixed value() {
        if (!data) fetch();
        return (array)data;
    }

    void set_index(int n) {
        page = n;
        data = 0;
    }

    int _sizeof() {
        if (!data) fetch();
        return (data->num_rows + (rows - 1)) / rows;
    }

    int num_rows() {
        if (!data) fetch();
        return data->num_rows;
    }

    int(0..1) `!() {
        return page >= sizeof(this);
    }

    int next() {
        page++;
        data = 0;
        return page < sizeof(this);
    }

    this_program `+=(int steps) {
        page += steps;
        data = 0;
        return this;
    }

    this_program `+(int ... steps) {
        this_program o = this_program(filter, order, rows);
        o->set_index(predef::`+(page, @steps));
        return o;
    }
}

//! @decl void select_complex(object filter, object order, object|function(int(0..1), array(mapping)|mixed:void) cb,
//!		      mixed ... extra)
//! @expr{order@} is an optional parameter allowing results to be ordered.
void select_complex(object filter, object order, object limit, mixed cb, mixed ... extra) {
    mixed rows;
    
    mixed err = catch {
        rows = low_select_complex(filter, order, limit);
    };

    if (!err) {
        cb(0, rows, @extra);
    } else {
	cb(1, err, @extra);
    }
}

int(0..) low_count_rows(void|object filter) {
    object sql = this_program::sql;
    int(0..) count;

    mixed err = sql_error(sql, catch {
            .Query index = objectp(filter) ? filter->encode_sql(this) : .Query("TRUE");
            array(mapping) rows;

            rows = (count_sql + index)(sql);

            count = (int)rows[0]->cnt;
    });

    if (err) throw(err);

    return count;
}

void count_rows(void|object filter, function(int(0..1),mixed,mixed...:void) cb, mixed ... extra) {
    object sql = this_program::sql;
    int(0..) count;

    mixed err = catch(count = low_count_rows(filter));

    if (err) {
        cb(1, err, @extra);
    } else {
        cb(0, count, @extra);
    }
}

void update(mapping keys, mapping|int version, function(int(0..1),mixed,mixed...:void) cb,
            mixed ... extra) {
    mixed err;
    array|mapping rows;
    object sql = this_program::sql;
    mapping t = ([]);
    int oversion, nversion;

    object where = get_where(keys);

    schema["version"]->encode_sql(table, ([ "version" : version ]), t);

    object uwhere = where + " AND " + gen_where(t);

    int locked = 0;
    int affected_rows = 0;

    foreach (schema->default_row; string s; mixed v) {
        if (has_index(keys, s) && objectp(keys[s]) && keys[s]->is_val_null)
            keys[s] = v;
    }

    err = sql_error(sql, catch {
	rows = (select_sql + where)(sql);
	if (sizeof(rows) != 1) error("%O\nCannot find 1 row, found %O\n", select_sql + where, rows);
	rows = rows[0];
        trigger("before_update", rows, keys);
	oversion = schema["version"]->decode_sql(table, rows);
	lock_tables(sql);
	locked = 1;
	mapping new = ([]);
	table_o->update(keys, rows, new);
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
	if (affected_rows == 1) {
            mapping new = sanitize_result(rows[0]);
            trigger("after_update", new, keys);
	    cb(0, new, @extra);
	} else {
	    cb(1, SyncDB.Error.Collision(this, nversion, oversion), @extra);
        }
    } else {
	cb(1, err, @extra);
    }
}

//! Remove deleted entries from db.
//! 
//! @note
//!     Does not currently work on linked tables.
void cleanup() {
    .Query index = .Filter.And(schema["version"]->Le(0))->encode_sql(this);
    (delete_sql + index)(sql);
}

void delete(mapping keys, mapping|int version, function(int(0..1),mixed,mixed...:void) cb,
            mixed ... extra) {
    int(0..1) noerr;
    mixed err;
    object sql = this_program::sql;

    int locked = 0;
    object where = get_where(keys);
    mapping t = ([]);

    schema["version"]->encode_sql(table, ([ "version" : version ]), t);

    object uwhere = where + " AND " + gen_where(t);

    int(0..1) real_delete = 0;

    foreach (schema->unique_fields();; object f) if (!f->is->automatic) {
        real_delete = 1;
        break;
    }

    trigger("before_delete", keys);

    // if the primary key is not automatic, we do a real delete
    if (!real_delete) {
        mapping d = ([ ]);
        d["version"] = -version;
        t = schema->encode_sql(table, d);

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
    } else {
	cb(1, err, @extra);
    }
}

array drop(void|object(SyncDB.MySQL.Filter.Base) filter) {
    filter &= schema["version"]->Gt(0);

    mixed err = sql_error(sql, catch {
        array rows = low_select_complex(filter, 0, 0);
        if (!sizeof(rows)) return rows;
        foreach (rows;; mapping row) trigger("before_delete", row);
        .Query q = delete_sql + filter->encode_sql(this);
        q(sql);
        foreach (rows;; mapping row) {
            trigger("after_delete", row);
            row->version = 0;
        }
        return rows;
    });

    throw(err);
}

// INSERT INTO table1(c1, c2, c3), table2(c4, c5, c6) VALUES ('v1', 'v2', 
// 'v3',v4, 'v5', 'v6'); 
//

.Query `lock_tables() {
    return .Query(sprintf("LOCK TABLES `%s` WRITE;", table_name()));
}

.Query `unlock_tables() {
    return .Query("UNLOCK TABLES;");
}

object(SyncDB.MySQL.Filter.Base) low_insert(array(mapping) rows) {
    object sql = this_program::sql;

    mapping def = schema->default_row;

    if (sizeof(def)) {
        foreach (rows;; mapping row) {
            foreach (def; string s; mixed v) {
                if (!has_index(row, s) || (objectp(row[s]) && row[s]->is_val_null))
                    row[s] = v;
            }
        }
    }

    foreach (rows;; mapping row)
        trigger("before_insert", row);

    array data = map(rows, table_o->insert);

    array(string) fields = indices(data[0]);

    data = Array.flatten(map(data, Function.curry(map)(fields)));

    object insert_sql = .Query("INSERT INTO `" + table_name() + "` (" + fields * "," + ") VALUES (" +
                               allocate(sizeof(rows), allocate(sizeof(fields), "%s") * ",") * "),(" +
                               ")");
    insert_sql->args = data;

    int locked = 0;

    mixed err = sql_error(sql, catch {
        object filter;
	lock_tables(sql); locked = 1;

        insert_sql(sql);

        // TODO: when inserting some rows with id, these checks will actually fail.
        if (schema->id) {
            object id_field = schema->id;

            if (id_field->is->automatic) {
                int last_id = sql->master_sql->insert_id();
                filter = id_field->Ge(last_id) & id_field->Lt(last_id + sizeof(rows));
            } else {
                filter = id_field->In(rows[id_field->name]);
            }
        }

        update_table_version(sql);

        unlock_tables(sql); locked = 0;

        foreach (rows;; mapping row)
            trigger("after_insert", row);
        return filter;
    });

    if (locked) unlock_tables(sql);

    throw(err);
}

void insert(mapping row, function(int(0..1),mixed,mixed...:void) cb, mixed ... extra) {
    mixed err;
    array rows;
    object sql = this_program::sql;

    mapping def = schema->default_row;


    err = catch {
        object f = low_insert(({ row }));
        rows = low_select_complex(f, 0, 0);
        row = rows[0];
    };

    if (!err) {
	cb(0, row, @extra);
    } else {
	cb(1, err, @extra);
    }
}

mixed sanitize_result(mixed rows) {
    if (mappingp(rows)) {
	return schema->decode_sql(table, rows);
    } else {
        return map(rows, Function.curry(schema->decode_sql)(table));
    }

}

string get_sql_name(string field) {
    return schema[field]->sql_name(table);
}
