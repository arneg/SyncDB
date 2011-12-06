// vim:foldmethod=syntax
UTIL.Base = Base.extend({
    M : function() {
	var l = [ this ].concat(Array.prototype.slice.call(arguments));
	return UTIL.make_method.apply(UTIL, l);
    }
});
/** @namespace */
SyncDB = {
    _tables : {},
    warn : function(err) {
	// support sprintf like syntax here!
	UTIL.log("SyncDB WARN: %o", err);
	UTIL.trace();
    },
    error : function(err) {
	UTIL.log("SyncDB ERROR: %o", err);
	UTIL.trace();
	throw(err);
    },
    Error : {
	NoSync : Base.extend({
	    toString : function () { return "NoSync"; },
	}),
	Set : Base.extend({
	    toString : function () { return "Set"; },
	}),
	NoIndex : Base.extend({
	    toString : function () { return "NoIndex"; },
	}),
	NotFound : Base.extend({
	    toString : function () { return "NotFound"; },
	}),
	Collision : Base.extend({
	    constructor : function(table, row, orow) {
		this.table = table;
		this.row = row;
		this.old_row = orow;
	    }
	}),
    },
    prune : function() {
	for (var i = 0; i < localStorage.length; i ++) {
	    var key = localStorage.key(i);
	    if (UTIL.has_prefix(key, "_syncdb_")) {
		delete localStorage[key];
	    }
	}

	return true;
    },
    logcb : function(cb, level) {
	return function(error, row) {
	    if (error) {
		UTIL.log("FAIL: %o %o\n", error, row);
		UTIL.trace();
	    } else if (level)
		UTIL.log("SUCC: %o\n", row);
	    cb.call(this, error, row);
	};
    },
    setcb : function(error, row) {
	if (!error) {
	    UTIL.log("SUCCESS: saved %o\n", row);
	} else {
	    UTIL.log("FAIL: could not save %o\n", error);
	    UTIL.trace();
	}
    },
    getcb : function(error, row) {
	if (!error) {
	    UTIL.log("FETCHED: %o\n", row);
	} else {
	    UTIL.log("FAIL: could not fetch %o\n", error);
	    UTIL.trace();
	}
    },
    Delete : Base.extend({
	constructor : function(schema, row) {
	    this.schema = schema;
	    if (row) {
		this[schema.key] = row[schema.key];
		this.version = row.version;
	    }
	},
    }),
    Undefined : undefined,
    Null : { toString : function() { return "!!!!! SQL NULL !!!!!"; } }
};
SyncDB.Version = function(a) {
    if (UTIL.arrayp(a))
	this.a = a;
    else if (UTIL.intp(a)) {
	this.a = new Array(a);
	for (var i = 0; i < a; i++) this.a[i] = 0;
    } else 
	this.a = Array.prototype.slice.call(arguments);
};
SyncDB.Version.prototype = {
    toString : function() {
	return "SyncDB.Version("+this.a.join(".")+")";
    },
    toArray : function() {
	return this.a;
    },
    lt : function(o) {
	var b = false;
	o = o.a;
	if (o.length != this.a.length) return false;

	for (var i = 0; i < this.a.length; i++) {
	    if (o[i] < this.a[i]) return false;
	    if (o[i] > this.a[i]) b = true;
	}

	return b;
    },
    gt : function(o) {
	var b = false;
	if (o.a.length != this.a.length) return false;

	for (var i = 0; i < this.a.length; i++) {
	    if (o.a[i] > this.a[i]) return false;
	    if (o.a[i] < this.a[i]) b = true;
	}

	return b;
    },
    eq : function(o) {
	if (o.a.length != this.a.length) return false;
	for (var i = 0; i < this.a.length; i++) 
	    if (o.a[i] != this.a[i]) return false;

	return true;
    },
    max : function(o) {
	o = o.a;
	if (o.length != this.a.length) UTIL.error("bad argument %o to max", o);
	var ret = new Array(o.length);
	for (var i = 0; i < this.a.length; i++) ret[i] = Math.max(o[i], this.a[i]);
	return new SyncDB.Version(ret);
    },
};
SyncDB.Row = function(schema) {
    this.schema = schema;
};
SyncDB.Row.prototype = {
    Delete : function() {
	return new (this.schema.Delete())(this);
    },
    Update : function(m) {
	var r = new (this.schema.Row())();

	for (var name in this.schema.m) 
	    if (this.schema.m.hasOwnProperty(name)) {
		var type = this.schema.m[name];
		if (m.hasOwnProperty(name)) {
		    if (!type.is_writable) UTIL.error("Trying to set readonly field %o", name);
		    r[name] = m[name];
		}
	    }

	r[this.schema.key] = this[this.schema.key];
	r.version = this.version;
	return r;
    }
};
/*
 * what do we need for index lookup in a filter:
 * - the index (matching one field)
 *  * needs to have right api for this filter (e.g. range lookups, etc)
 * - hence it needs the schema
 *
 */
/** @namespace */
SyncDB.Filter = {};
SyncDB.Filter.Base = UTIL.Base.extend({
    _types : {
	field : new serialization.String()
    },
    constructor : function(field, parser) {
	this.field = field;
	this.args = Array.prototype.slice.apply(arguments);
	this.parser = parser;
    },
    index_insert : function() {
	return [];
    },
    filter : function(rows) {
	return [];
    }
});
SyncDB.Filter.And = SyncDB.Filter.Base.extend({
    _types : {
	args : function(p) {
	    return new serialization.Array(p);
	}
    },
    index_lookup : function(table) {
	var results = this.args[0].index_lookup(table);

	for (var i = 1; i < this.args.length; i++) {
	    if (!results.length) break;
	    var t = this.args[i].index_lookup(table);
	    if (!t) return 0;
	    results = UTIL.array_and(t, results);
	}

	return results;
    }
});
SyncDB.Filter.Or = SyncDB.Filter.And.extend({
    index_lookup : function(table) {
	var results = this.args[0].index_lookup(table)

	for (var i = 1; i < this.args.length; i++) {
	    results = UTIL.array_or(this.args[i].index_lookup(table), results);
	}

	return results;
    },
    index_insert : function(table, rows) {
	var results = [];

	for (var i = 0; i < this.args.length; i++) {
	    var r = this.args[i].filter(table.schema, rows);
	    if (r.length)
		results = UTIL.array_or(this.args[i].index_insert(table, r), results);
	}

	return results;
    }
});
SyncDB.Filter.False = SyncDB.Filter.Base.extend({
    index_lookup : function(table) {
	return [];
    }
});
SyncDB.Filter.Equal = SyncDB.Filter.Base.extend({
    _types : {
	field : new serialization.String(),
	value_atom : new serialization.Any()
    },
    value_atom : function() {
	return this.parser.encode(this.value);
    },
    constructor : function(field, parser, value) {
	this.base(field, parser);
	this.value = value;
    },
    index_lookup : function(table) {
	if (table.I[this.field]) {
	    return table.I[this.field].lookup_equal(this.value);
	} else UTIL.error("no index for %s", this.field);
    },
    filter : function(schema, rows) {
	var type = schema.m[this.field];
	var ret = [];
	for (var i = 0; i < rows.length; i++)
	    if (rows[i][this.field] == this.value) ret.push(rows[i]);
	return ret;
    },
    index_insert : function(table, rows) {
	var index;
	if (index = table.I[this.field]) {
	    for (var i = 0; i < rows.length; i++) {
		index.insert(rows[i][this.field], rows[i][table.schema.key]);
	    }
	    return rows;
	} else return [];
    }
});
SyncDB.Filter.True = SyncDB.Filter.Base.extend({
    constructor : function(field) {
	this.field = field;
    },
    index_lookup : function(table) {
	return table.I[this.field].values();
    }
});
SyncDB.Filter.Overlaps = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.overlaps)
	    throw(new SyncDB.Error.NotFound());
	return index.overlaps(this.value);
    }
});
SyncDB.Filter.Contains = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.lookup_contains)
	    throw(new SyncDB.Error.NotFound());
	return index.lookup_contains(this.value);
    }
});
SyncDB.Filter.Gt = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.lookup_gt)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_gt(this.value);
    }
});
SyncDB.Filter.Ge = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.lookup_ge)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_ge(this.value);
    }
});
SyncDB.Filter.Lt = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.lookup_lt)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_lt(this.value);
    }
});
SyncDB.Filter.Le = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var index = table.I[this.field];
	if (!index || !index.lookup_le)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_le(this.value);
    }
});
/** @namespace */
SyncDB.Serialization = {
    Null : new serialization.Singleton("_null", SyncDB.Null),
    Undefined : new serialization.Singleton("_undefined", SyncDB.Undefined),
};
SyncDB.Serialization.Filter = serialization.generate_structs({
    _or : SyncDB.Filter.Or,
    _and : SyncDB.Filter.And,
    _equal : SyncDB.Filter.Equal,
    _true : SyncDB.Filter.True,
    _false : SyncDB.Filter.False,
    _overlaps : SyncDB.Filter.Overlaps,
    _contains : SyncDB.Filter.Contains,
    _le : SyncDB.Filter.Le,
    _ge : SyncDB.Filter.Ge,
    _lt : SyncDB.Filter.Lt,
    _gt : SyncDB.Filter.Gt
});
SyncDB.KeyValueMapping = UTIL.Base.extend({
    constructor : function() {
	this.m = {};
    },
    is_permanent : false,
    set : function(key, value, cb) {
	this.m[key] = value;
	UTIL.call_later(cb, null, false, value);
    },
    cas : function(key, value, old, cb) {
	if (this.m[key] == old) {
	    this.m[key] = value;
	    UTIL.call_later(cb, null, false, value);
	} else {
	    UTIL.call_later(cb, null, true, "Collision");
	}
    },
    get : function(key, cb) {
	if (UTIL.arrayp(key)) {
	    for (var i = 0; i < key.length; i++) key[i] = this.m[key[i]];
	} else key = this.m[key];
	UTIL.call_later(cb, null, false, key);
    },
    remove : function(key, cb) {
	var v = this.m[key];
	delete this.m[key];
	UTIL.call_later(cb, null, false, v);
    },
    size : function(cb) {
	var size = 0;
	for (var key in this.m) if (this.m.hasOwnPropert(key)) {
	    size += key.length + this.m[key].length;
	}
	UTIL.call_later(cb, null, false, size);
    },
    clear : function(cb) {
	this.m = {};
	UTIL.call_later(cb, null, false);
    },
    toString : function() {
	return "SyncDB.KeyValueMapping";
    }
});
if (UTIL.App.has_local_storage) {
    SyncDB.KeyValueStorage = UTIL.Base.extend({
	constructor : function(prefix) {
	    this.prefix = prefix || "";
	},
	set : function(key, value, cb) {
	    try {
		localStorage[this.prefix+key] = value;
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		UTIL.call_later(cb, null, err);
	    }
	},
	cas : function(key, value, old, cb) {
	    if (localStorage[this.prefix+key] == old) {
		localStorage[this.prefix+key] = value;
		cb(false, value);
	    } else {
		cb(true, "Collision!");
	    }
	},
	is_permanent : true,
	get : function(key, cb) {
	    try {
		if (UTIL.arrayp(key)) {
		    for (var i = 0; i < key.length; i++)
			key[i] = localStorage[this.prefix+key[i]];
		} else 
		    key = localStorage[this.prefix+key];
		UTIL.call_later(cb, null, false, key);
	    } catch(err) {
		UTIL.call_later(cb, null, err);
	    }

	},
	remove : function(key, cb) {
	    try {
		var value = localStorage[this.prefix+key];
		delete localStorage[this.prefix+key];
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		UTIL.call_later(cb, null, err);
	    }
	},
	size : function(cb) {
	    var size = 0;
	    for (i = 0; i < localStorage.length; i++) {
		var key = localStorage.key(i);
		if (key.search(this.prefix) == 0) {
		    size += key.length + localStorage[key].length;
		}
	    }
	    UTIL.call_later(cb, null, false, size);
	},
	clear : function(cb) {
	    // TODO: remove only prefix LS entries
	    var r = [], i;
	    for (i = 0; i < localStorage.length; i++) {
		var key = localStorage.key(i);
		if (key.search(this.prefix) == 0) {
		    r.push(key);
		}
	    }
	    for (i = 0; i < r.length; i++) {
		localStorage.removeItem(r[i]);
	    }
	    UTIL.call_later(cb, null, false);
	},
	toString : function() {
	    return "SyncDB.KeyValueStorage";
	}
    });
}
if (UTIL.App.is_ipad || UTIL.App.is_phone || UTIL.App.has_local_database) {
    SyncDB.KeyValueDatabase = UTIL.Base.extend({
	constructor : function(prefix, cb) {
	    this.db = openDatabase("SyncDB:"+prefix, "1.0", "SyncDB", 5*1024*1024);
	    this.init(cb);
	    this.q = [];
	    this.Q = [ this.q ];
	},
	init : function(cb) {
	    try {
		this.db.transaction(this.M(function (tx) {
		    try {
			tx.executeSql("CREATE TABLE IF NOT EXISTS sLsA (key VARCHAR(255) PRIMARY KEY, value BLOB);", [],
				      this.M(function(tx, data) {
					this.M(cb)(false);
					this.run(true);
				      }),
				      this.M(function(tx, err) {
					this.M(cb)(err);
				      }));
		    } catch(err) {
			this.M(cb)(err);
		    }
		}));
		this.running = true;
	    } catch (err) {
		this.M(cb)(err);
	    }
	},
	size : function(cb) {
	    this.push("select sum(length(key)) as s1, sum(length(value)) as s2 from sLsA;", [],
		      function(tx, data) {
			  if (data.rows.length != 1) {
			      cb(true, "got only "+data.rows.length+"rows");
			  } else {
			     cb(false, parseInt(data.rows.item(0).s1) + parseInt(data.rows.item(0).s2));
			  }
		      },
		      function(tx, err) {
			  cb(err);
		      });
	},
	is_permanent : true,
	_wrap : function(cb1, cb2) {
	    return function(tx, data) {
		cb1(tx, data);
		cb2(tx, data);
	    };
	},
	push_unsafe : function(a, b, c, d) {
	    if (this.q) {
		this.Q.push(this.q);
		this.q = null;
	    }
	    this.Q.push([[ a,b,c,d ]]);
	    if (!this.running) this.run();
	},
	push : function(a, b, c, d) {
	    if (this.q && this.q.length < 1000) 
		this.q.push([a, b, c, d]);
	    else {
		this.q = [[ a, b, c, d ]];
		this.Q.push(this.q);
	    }
	    if (!this.running) this.run();
	},
	run : function(force) {
	    var err;
	    if (!force && this.running) return;
	    if (this.Q.length && this.Q[0].length) {
		this.running = true;
		err = this.M(function(err) { UTIL.error("err: %o", err); });
		this.db.transaction(this.M(this._transaction), err, this.M(this.run, true));
	    } else {
		this.running = false;
	    }
	},
	_transaction : function(tx) {
	    var ea = new UTIL.EventAggregator();
	    var q = this.Q.shift();
	    if (this.q === q) {
		this.q = null;
	    }
	    var t = new Date();
	    console.log("executing: %d statements", q.length);
	    for (var i = 0; i < q.length; i++) {
		var cb = ea.get_cb();
		tx.executeSql(q[i][0], q[i][1],
			      this._wrap(q[i][2], cb,
			      this._wrap(q[i][3], cb)));
	    }
	    ea.ready(function() { console.log("done in %o ms", new Date() - t); });
	    ea.start();
	},
	get : function(key, cb) {
	    cb = UTIL.safe(cb);
	    var i, arr = UTIL.arrayp(key);
	    var query = "SELECT * FROM sLsA WHERE ";
	    var args;
	    if (arr) {
		query += "key in (";
		if (key.length < 1000) {
		    query += UTIL.nchars("?,", key.length - 1)+"?";
		    args = key;
		} else {
		    var t = new Array(key.length);
		    for (var i = 0; i < key.length; i++) {
			t[i] = key[i].replace(/\\/g, "\\\\").replace(/'/g, "\\'");
		    }
		    query += "'"+t.join("','")+"'";
		    args = [];
		}
		query += ");";
	    } else {
		query += "key=?;";
		args = [ key ];
	    }

	    this.push(query, args,
			this.M(function(tx, data) {
		    var ret = arr ? new Array(key.length) : null;
		    var m = {};
		    for (i = 0; i < data.rows.length; i++) m[data.rows.item(i).key] = this.decode(data.rows.item(i).value);
		    if (arr) {
			for (i = 0; i < key.length; i++) ret[i] = m[key[i]];
		    } else ret = m[key];
		    cb(false, ret);
		}), function(tx, err) {
		    cb(err);
		});
	},
	cas : function(key, val, old, cb, foo) {
	    cb = UTIL.safe(cb);
	    var good = function(tx, data) {
		    if (data.rowsAffected != 1) {
			cb(true, "cas failed!");
		    } else cb(false, val);
		};
	    var bad = function (tx, err) {
		    cb(err);
		};
	    if (old) {
		this.push_unsafe("UPDATE sLsA SET value=? WHERE key=? AND"+
			      " value=?;",
			      [ this.encode(val), key, this.encode(old) ], good, bad);
	    } else
		this.push_unsafe("INSERT INTO sLsA (key, value)"+
			      " VALUES(?, ?);",
			      [ key, this.encode(val) ], good, bad);
	},
	hash : function(s) {
	    return ((new UTIL.SHA256.Hash()).update(s).hex_digest());
	},
	set : function(key, val, cb) {
	    cb = UTIL.safe(cb);
	    this.push("INSERT OR REPLACE INTO sLsA (key, value) VALUES(?, ?);", [ key, this.encode(val) ],
			  function(tx, data) {
			    if (data.rowsAffected != 1) cb(true, "Collision");
			    else cb(false, val);
			  },
			  function (tx, err) {
			    cb(err);
			  });
	},
	remove : function(key, cb) {
		cb = UTIL.safe(cb);
		this.push("SELECT * FROM sLsA WHERE key=?;", [key],
		    this.M(function(tx, data) {
			this.push("DELETE FROM sLsA WHERE key=?;", [key],
				      this.M(function (tx) {
					  cb(false, this.decode(data.rows.item(0).value));
				      }),
				      function (tx, err) {
					  cb(err);
				      });
				      }),
		    function (tx, err) {
			cb(err);
		    });
	},
	encode : function(s) {
	    return UTF8.encode(s.replace(/\0/g, "\u0100"));
	},
	decode : function(s) {
	    return UTF8.decode(s).replace(/\u0100/g, "\0");
	},
	clear : function(cb) {
	    cb = UTIL.safe(cb);
	    this.push("DROP TABLE sLsA;", [], this.M(function() {
			  this.init(cb);
		      }), function(err) {
			  cb(err);
		      });
	},
	toString : function() {
	    return "SyncDB.KeyValueDatabase";
	}
    });
}
SyncDB.LS = function(prefix) {
    if (SyncDB.KeyValueDatabase)
	return new SyncDB.KeyValueDatabase(prefix, function (err) {
	    if (err) {
		UTIL.error("failed to initialize local database. fallback deactivated right now! (%o)", err);
		// rewrite this here with functions from a fallback!
		//SyncDB.LS = new (SyncDB.KeyValueStorage || SyncDB.KeyValueMapping)();
	    }
	});
    if (SyncDB.KeyValueStorage) {
	if (window.JSON && !UTIL.App.has_indexedDB) {
	    return new (SyncDB.KeyValueMapping.extend({
		constructor : function(prefix) {
		    this.prefix = prefix;
		    this.field = "syncdb_ls_"+prefix;
		    this.m = JSON.parse(localStorage[this.field] || "{}");
		    this.sync = UTIL.make_method(this, function() {
			this.will_sync = false;
			localStorage[this.field] = JSON.stringify(this.m);
		    });
		    this.will_sync = false;
		},
		set : function(key, value, cb) {
		    if (!this.will_sync) {
			this.will_sync = true;
			UTIL.call_later(this.sync);
		    }
		    this.base(key, value, cb);
		},
		cas : function(key, val, oval, cb) {
		    if (!this.will_sync) {
			this.will_sync = true;
			UTIL.call_later(this.sync);
		    }
		    this.base(key, val, oval, cb);
		},
		clear : function(cb) {
		    if (!this.will_sync) {
			this.will_sync = true;
			UTIL.call_later(this.sync);
		    }
		    this.base(cb)
		},
		is_permanent : true
	    }))(prefix);
	}
	return new SyncDB.KeyValueStorage(prefix);
    } else return new SyncDB.KeyValueMapping(prefix);
};
SyncDB.LocalField = UTIL.Base.extend({
    constructor : function(ls, name, parser, def) {
	this.ls = ls;
	this.name = name;
	this.parser = parser;
	this.value = undefined;
	this.def = def;
	this.will_set = false;
	this.auto_sync = true;
	// this.get is overloaded and might be synchronous only (e.g. Index)
	SyncDB.LocalField.prototype.get.call(this, function() {
	    //UTIL.log("initialized field %s", this.name);
	});
	//UTIL.log("name: %s, parser: %o\n", name, parser);
    },
    get : function(cb) {
	if (!cb) UTIL.error("CallBack missing.");
	cb = UTIL.safe(cb);

	if (!this.value) { // cache this, we will fetch
	    if (this.get_queue) {
		this.get_queue.push(cb);
		return;
	    }
	    this.get_queue = [ cb ];
	    this.ls.get(this.name, this.M(function(err, value) {
		var ret;
		if (err) {
		    ret = undefined;
		} else {
		    if (value)
			this.value = this.parser.decode(serialization.parse_atom(value));
		    else if (this.def) {
			//UTIL.log("setting default to %o", this.def);
			this.value = this.def;
			delete this.def;
			this.sync();
		    }
		    ret = this.value;
		}
		var q = this.get_queue;
		delete this.get_queue;
		for (var i = 0; i < q.length; i++) q[i](ret);
	    }));
	    return this;
	}
	cb(this.value);
	//UTIL.call_later(cb, null, this.value);
	return this;
    },
    set : function(value) {
	//UTIL.log("name: %o, parser: %o, this: %o\n", this.name, this.parser, this);
	if (this.def) {
	    delete this.def;
	}
	if (!value) UTIL.trace();
	this.value = value;
	this.sync();
    },
    autosync : function(t) {
	t = !!t;
	if (t === this.auto_sync)
	    return;
	this.auto_sync = t;
	if (t) {
	    this.will_set = false;
	    this.sync();
	}
    },
    sync : function() {
	// We want to allow for looping over a repeated set call (e.g. MultiIndex)
	if (this.will_set) return;
	this.will_set = true;

	if (!this.auto_sync) return;

	UTIL.call_later(function() {
		this.will_set = false;
		if (this.value == undefined) {
		    this.ls.remove(this.name, function () {});
		} else {
		    // TODO: use cas here to catch errors in case someone
		    // uses the syncdb twice at one time!
		    this.ls.set(this.name, this.parser.encode(this.value).render(), function () {});
		}
	    }, this);
    }
});
SyncDB.Index = SyncDB.LocalField.extend({
    constructor : function(ls, name, tkey, tid) {
	this.tkey = tkey;
	this.tid = tid;
	//console.log("SyncDB.Index constructor.");
	this.base(ls, name, new serialization.Object(tkey.parser()), {});
	this.get(UTIL.make_method(this, function() { this.init.apply(this, Array.prototype.slice.call(arguments)); }));
    },
    init : function() {},
    insert : function(key, id) {
	this.value[id] = key;
	this.sync();
    },
    remove : function(key, id) {
	// keep track of deletes, so we know how bad our filter got.
	// regenerate as needed.
	delete this.value[id];
	this.sync();
    },
    values : function() {
	var l = UTIL.keys(this.value);
	for (var i = 0; i < l.length; i++) l[i] = this.tid.fromString(l[i]);
	return l;
    },
    has : function() {
	return true;
    },
    get_filter : function(filters) {
	if (!filters) filters = [];
	return filters;
    },
    get_filter_parser : function(parsers) {
	if (!parsers) parsers = [];
	return parsers;
    }
});
SyncDB.RangeFilter = {
    init : function(m) {
	this._regen_range(m);
	this.base(m);
    },
    _regen_range : function(m) {
	this.rangefilter = new CritBit.RangeSet();
	for (var id in m) if (m.hasOwnProperty(id)) {
	    this.rangefilter.insert(m[id]);
	}
    },
    insert : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	this.rangefilter.insert(index);
	this.base(index, id);
    },
    get_filter : function(filters) {
	filters = this.base(filters);
	filters.push(this.rangefilter);
	return filters;
    },
    has : function(index) {
	if (index instanceof CritBit.Range)
	    return this.rangefilter.contains(index);
	// should not always be true!
	return this.base(index);
    },
    get_filter_parser : function(parser) {
	parser = this.base(parser);
	parser.push(new serialization.RangeSet(this.tkey.parser()));
	return parser;
    },
    remove : function(key, id) {
	this.base(key, id);
	this._regen_range(this.value);
    }
};
SyncDB.RangeIndex = {
    init : function(m) {
	this.m = new CritBit.MultiRangeSet();
	for (var id in m) if (m.hasOwnProperty(id)) {
	    var index = m[id];
	    index.value = this.tkey.fromString(id);
	    this.m.insert(index);
	}
	this.base(m);
    },
    insert : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	index.value = id;
	this.m.insert(index);
	this.base(index, id);
    },
    overlaps : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	var a = this.m.overlaps(index);
	if (!a.length) return [];
	var ret = new Array(a.length);
	for (var i = 0; i < a.length; i ++) {
	    ret[i] = a[i].value;
	}
	return ret;
    },
    lookup_equal : function(index) {
	return this.overlaps(index);
    },
    remove : function(key, id) {
	this.m.remove(key);
	this.base(key, id);
    }
};
// TODO: this should really be a subindex that doesnt do lookup_get, only
// range lookups and has a RangeSet for the cached entries.
SyncDB.CritBitIndex = {
    init : function(m) {
	this.m = new CritBit.Tree();
	for (var id in m) if (m.hasOwnProperty(id)) {
	    this.m.insert(m[id], this.tkey.fromString(id));
	}
	this.base(m);
    },
    insert : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	this.m.insert(index, id);
	this.base(index, id);
    },
    index_equal : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.m.hasOwnProperty(index)) return [];
	return [ this.m.index(index) ];
    },
    remove : function(key, id) {
	this.m.remove(key);
	this.base(key, id);
    },
    values : function() {
	return this.m.values();
    },
    lookup_contains : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) throw(new SyncDB.Error.NotFound());
	var a = [];
	this.m.foreach(function(key, val) { a.push(val); },
			     index.a, index.b);
	return a;
    },
    lookup_lt : function(val) {
	var r = [];
	var node = this.m.lt(val);
	while (node && node.value < val) {
	    r.push(node.value);
	    node = node.backward();
	}
	return r;
    },
    lookup_le : function(val) {
	var r = [];
	var node = this.m.le(val);
	while (node && node.value <= val) {
	    r.push(node.value);
	    node = node.backward();
	}
	return r;
    },
    lookup_gt : function(val) {
	var r = [];
	var node = this.m.gt(val);
	while (node && node.value > val) {
	    r.push(node.value);
	    node = node.forward();
	}
	return r;
    },
    lookup_ge : function(val) {
	var r = [];
	var node = this.m.ge(val);
	while (node && node.value >= val) {
	    r.push(node.value);
	    node = node.forward();
	}
	return r;
    }
};
SyncDB.MappingIndex = {
    init : function(o) {
	this.m = {};
	for (var id in o) if (o.hasOwnProperty(id)) {
	    this.m[o[id]] = this.tid.fromString(id);
	}
	this.base(o);
    },
    lookup_equal : function(index) {
	if (!this.value) SyncDB.error("You are too early!!");
	if (!this.m.hasOwnProperty(index)) return [];
	return [ this.m[index] ];
    },
    insert : function(key, id) {
	this.m[key] = id;
	this.base(key, id);
    },
    remove : function(key, id) {
	// keep track of deletes, so we know how bad our filter got.
	// regenerate as needed.
	delete this.m[key];
	this.base(key, id);
    },
    values : function() {
	// optimized version here!
	return this.base();
    }
};
SyncDB.MappingFilter = {
    has : function(index) {
	return this.m.hasOwnProperty(index.toString());
    }
};
SyncDB.BloomFilter = {
    init : function(m) {
	this.base(m);
	this._regen_bloom(m);
    },
    insert : function(key, id) {
	if (this.bloomfilter.set(key))
	    this.regen_filter();
	this.base(key, id);
    },
    remove : function(key, id) {
	if (this.bloomfilter.remove(key)) {
	    this.regen_filter();
	}
	this.base(key, id);
    },
    _regen_bloom : function(m) {
	var l = UTIL.keys(m);
	this.bloomfilter = new UTIL.Bloom.Filter(l.length, 0.001, this.tkey.hash());
	for (var i = 0; i < l.length; i++) {
	    this.bloomfilter.set(m[l[i]]);
	}
    },
    regen_filter : function() {
	// decide when to regenerate here. Lets say, we 
	var p = this.bloomfilter.prob();
	if (p > 0.002) {
	    this._regen_bloom(this.value);
	}
    },
    get_filter : function(filters) {
	filters = this.base(filters);
	filters.push(this.bloomfilter);
	return filters;
    },
    get_filter_parser : function(parser) {
	parser = this.base(parser);
	parser.push(new serialization.Bloom(this.tkey.hash()));
	return parser;
    }
};
SyncDB.MultiIndex = {
    init : function(m) {
	this.m = {};
	for (var id in m) if (m.hasOwnProperty(id)) {
	    if (!this.m.hasOwnProperty(m[id]))
		this.m[m[id]] = {};
	    this.m[m[id]][id] = 1;
	}
	this.base(m);
    },
    insert : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.m.hasOwnProperty(index)) {
	    this.m[index] = { };
	}
	this.m[index][id] = 1;
	// adding something can be done cheaply, by appending the tuple
	//UTIL.log(">!> %o", this.value);
	this.base(index, id);
    },
    lookup_equal : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.m.hasOwnProperty(index)) return [];
	var l = UTIL.keys(this.m[index]);
	for (var i = 0; i < l.length; i++) l[i] = this.tkey.fromString(l[i]);
	return l;
    },
    remove : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (this.m.hasOwnProperty(index)) {
	    delete this.m[index][id];
	    if (!this.m[index].length) delete this.m[index];
	}
	this.base(index, id);
    }
};
SyncDB.TrueFilter = {
    has : function() { return true; }
};
SyncDB.FalseFilter = {
    has : function() { return false; }
};
SyncDB.Schema = UTIL.Base.extend({
    constructor : function() {
	this.m = {};
	this.fields = Array.prototype.slice.call(arguments);
	this.autos = [];
	for (var i = 0; i < arguments.length; i++) {
	    var type = arguments[i];
	    var name = type.name;
	    this.m[name] = type;
	    if (type.is_key) {
		this.id = type;
		this.key = name;
	    }
	    if (type.get_val) this.autos.push(name);
	}
	this.schema_parser = UTIL.cached(this.schema_parser);
	this.parser_in = UTIL.cached(this.parser_in);
	this.parser_out = UTIL.cached(this.parser_out);
    },
    parser_in : function() {
	return this.parser(function(name, type) {
	    return type.is_readable && !type.is_hidden;
	});
    },
    parser_out : function() {
	return this.parser(function(name, type) {
	    return type.is_writable && !type.is_automatic;
	});
    },
    hashCode : function() {
	return (new UTIL.SHA256.Hash()).update(this.schema_parser().encode(this).render()).hex_digest();
    },
    schema_parser : function() {
	return new SyncDB.Serialization.Schema();
    },
    Insert : function(m) {
	var r = new (this.Row())();
	for (var name in this.m) if (this.m.hasOwnProperty(name)) {
	    var type = this.m[name];
	    if (m.hasOwnProperty(name)) {
		if (!type.is_writable)
		    UTIL.error("Trying to insert readonly field %o.", type);
		r[name] = m[name];
	    } else if (type.is_mandatory) {
		UTIL.error("Missing mandatory field %o.", type);
	    }
	}
	return r;
    },
    Delete : function() {
	if (this._Delete) return this._Delete;
	var schema = this;
	return this._Delete = SyncDB.Delete.extend({
	    constructor : function(row) {
		this.base(schema, row);
	    }
	});
    },
    Row : function() {
	if (this._Row) return this._Row;
	var schema = this;
	this._Row = function() {
	    this.schema = schema;
	};
	this._Row.prototype = SyncDB.Row.prototype;
	return this._Row;
    },
    parser : function(filter) {
	var n = {};
	for (var name in this.m) if (this.m.hasOwnProperty(name)) {
	    if (name == "version" || name == this.key ||
		!filter || filter(name, this.m[name])) {
		var t = [ this.m[name].parser(),
			  SyncDB.Serialization.Null ];
		if (!this.m[name].is_mandatory)
		    t.push(serialization.Undefined);
		n[name] = new serialization.Or(t);
	    }
	}
	var m = {};
	m.version = n.version;
	m[this.key] = n[this.key];
	return new serialization.Or(
		    new serialization.Struct("_row", n, this.Row()),
		    new serialization.Struct("_delete", m, this.Delete())
		    );
    },
    get_auto_set : function(db, cb) {
	var as = {};
	var cnt = 1;
	for (var i = 0; i < this.autos.length; ++i) {
	    var name = this.autos[i];
	    ++cnt;
	    this.m[name].get_val(db, name, this.m[name], function(val) {
		as[name] = val;
		if (!--cnt) cb(as);
	    });
	}

	if (!--cnt) {
	    cb(as);
	}
    }
});
SyncDB.TableConfig = SyncDB.LocalField.extend({
    constructor : function(ls, name) {
	// create self updating field with given serialization for
	// the schema and version, etc.
	this.base(ls, name, new serialization.Struct(null, {
			    version : new serialization.Array(new serialization.Integer()),
			    schema : new SyncDB.Serialization.Schema()
			}),
		  {
		    version : [],
		    schema : new SyncDB.Schema()
		  });
    },
    toString : function() {
	return "TableConfig()";
    },
    version : function() { // table version. to sync missing upstream revisions
	if (arguments.length) {
	    this.value.version = arguments[0].a;
	    this.sync();
	}
	return new SyncDB.Version(this.value.version);
    },
    schema : function() {
	if (arguments.length) {
	    this.value.schema = arguments[0];
	    this.sync();
	}
	//UTIL.trace();
	return this.value.schema;
    }
});
// NOTE:
//
// 	localtables have seperate logic for chaining, some is also
// 	in SyncDB.Table. This needs to be cleaned up. It should
// 	not make a difference what we are chaining.
//
//	Consequently, if we want to do drafting for things that
//	have not been synced up the chain, that means that in principle
//	all database types (meteor, localdb, localstorage, mapping) need
//	some kind of draft support. Since they can not depend on each other
//	we have to make drafting 'explicit'. Creating a pure meteortable does
//	not do any drafting, i.e. drafts are just stored in a mapping. LocalStorage
//	and LocalDatabase create drafts in permanent storage. We could think about
//	some kind of fallback using cookies for the meteortable.
//
//	Those temporary drafts should be available through some kind of standard
//	interface. db.get_drafts() / db.drafts.select_by_email() / ...
//	This should be created automatically by the SyncDB.Table class in the same way
//	we are doing it for the other getters and setters. Also we need callbacks for
//	events. They need to be handed down the chain.
SyncDB.Table = UTIL.Base.extend({
    constructor : function(name, schema, db) {
	this.changes = [];
	if (db) db.onchange(this.M(this.sync));
	this.name = name;
	this.schema = schema;
	this.parser = schema.parser();
	this.parser_in = schema.parser_in();
	this.parser_out = schema.parser_out();
	this.db = db;
	this.ready_ea = new UTIL.EventAggregator();
	UTIL.call_later(this.ready_ea.start, this.ready_ea);
	//if (db) db.add_update_callback(this.M(this.update));
	//UTIL.log("schema: %o\n", schema);
	var key = schema.key;

	if (!key) SyncDB.error(SyncDB.Error.Retard("Man, this schema wont work.\n"));

	for (var i = 0; i < schema.fields.length; i++) {
	    var type = schema.fields[i];
	    var field = type.name;
	    //UTIL.log("scanning %s:%o.\n", field, schema[field]);
	    if (type.is_indexed) {
		//UTIL.log("   is indexed.\n");


		this["select_by_"+field] = this.generate_select(field, type);
		if (type.is_key) {
		    //UTIL.log("   is key.\n");
		    this.select_by = this["select_by_"+field];
		    this.remove_by
		      = this["remove_by_"+field] =
			this.generate_remove(field, type);
		}

	    }
	}
    },
    ready : function(f) {
	this.ready_ea.ready(f);
    },
    onchange : function(cb) {
	this.changes.push(cb);
    },
    get_version : function() {},
    // I am not sure what this thing was supposed to do.
    sync : function(rows) {
	for (var i = 0; i < this.changes.length; i++)
	    UTIL.call_later(this.changes[i], this, rows);
    },
    index : function() {
	return null;
    },
    generate_remove : function(name, type) {
	var remove = this.remove(name, type);
	var db = this.db;
	if (!remove) SyncDB.error("could not generate remove() for %o %o\n", name, type);
	if (db) { return function(key, callback) {
	    db["remove_by_"+name](key, this.M(function(error, row) {
		if (error) return callback(error);

		remove(key, function(lerror, lrow) {
		    if (lerror) // this case is stupid. local db is out of sync
			callback(lerror);
		    else
			callback(false, row);
		});
	    }));
	} } else return remove;
    },
    generate_select : function(name, type) {
	return function(value, callback) {
	    return this.select(type.Equal(value), callback);
	};
    },
    insert : function(row, callback) {
	if (!(row instanceof this.schema.Row()))
	    row = this.schema.Insert(row);
	if (this.db) {
	    this.db.insert(row, callback);
	} else {
	    this.auto_set(row, this.M(function(n) {
		this.low_insert(n, callback);
	    }));
	}
    },
    auto_set : function(row, cb) {
	this.schema.get_auto_set(this, function(as) {
	    if (as.length) {
		row = UTIL.copy(row);
		for (var x in as) row[x] = as[x];
	    }
	    cb(row);
	});
    },
    select : function(filter, callback) {
	var extra = Array.prototype.slice.call(arguments, 2);
	// allow us to generate partial results at least.
	if (!callback) callback = SyncDB.getcb;
	callback = UTIL.once(callback);

	var f = extra.length ? function(error, rows) {
	    callback.apply(window, [error, row].concat(extra));
	} : callback;

	this.low_select(filter, this.M(function(error, rows) {
	    //UTIL.log("low_select -> %o %o", error, rows);
	    if (error instanceof SyncDB.Error.NoSync && this.db) {
		if (this.update_index)
		    this.db.select(filter, this.M(function(error, rows) {
			if (!error) this.update_index(filter, rows);
			f(error, rows);
		    }));
		else
		    this.db.select(filter, f);
		return;
	    }
	    f(error, rows);
	}));
    },
    update : function(row, callback, orow) {
	if (!orow) UTIL.error("you need to specify the row your update is based on!");
	if (!(row instanceof this.schema.Row()))
	    row = orow.Update(row);
	if (this.db) {
	    return this.db.update(row, callback, orow);
	} 
	return this.low_update(row, callback, orow);
    },
    version : function() {
	return this.config.version();
    },
    add_update_callback : function(cb) {
	// this gets triggered on update / delete
    }
});
/** @namespace */
SyncDB.Meteor = {
    Error : Base.extend({
	constructor : function(id, error) {
	    this.id = id;
	    this.error = error;
	}
    }),
    Base : Base.extend({
	constructor : function(id, row) {
	    this.id = id;
	    this.row = row;
	}
    }),
    Select : Base.extend({
	constructor : function(id, filter) {
	    this.id = id;
	    this.filter = filter;
	}
    }),
    Reply : Base.extend({
	constructor : function(id, rows) {
	    this.id = id;
	    this.rows = rows;
	}
    }),
    Update  : Base.extend({
	constructor : function(id, row, version, key) {
	    this.id = id;
	    this.row = row;
	    this.version = version;
	    this.key = key;
	}
    })
};
SyncDB.Meteor.Insert = SyncDB.Meteor.Base.extend({});
SyncDB.Meteor.Sync = SyncDB.Meteor.Base.extend({});
SyncDB.Meteor.SyncReq = SyncDB.Meteor.Base.extend({
    constructor : function(id, version, filter) {
	this.id = id;
	this.version = version;
	this.filter = filter;
    }
});

SyncDB.MeteorTable = SyncDB.Table.extend({
    constructor : function(name, schema, channel, db) {
	this.requests = {};
	this.channel = channel;
	this.atom_parser = new serialization.AtomParser();
	this.base(name, schema, db);
	var version_type = schema.m.version.parser();
	var s = new serialization.String();
	var regtype = function(poly, atype, ptype, m) {
	    poly.register_type(atype, ptype,
			       new serialization.Struct(atype, m, ptype));
	};
	this.incoming = new serialization.Polymorphic();
	regtype(this.incoming, "_error", SyncDB.Meteor.Error,
		{ id : s, error : s });
	regtype(this.incoming, "_reply", SyncDB.Meteor.Reply,
		{ id : s, rows : new serialization.Array(this.parser_in) });
	regtype(this.incoming, "_sync", SyncDB.Meteor.Sync,
		{ id : s, rows : new serialization.Array(this.parser_in) });
	this.out = new serialization.Polymorphic();
	var m = {};
	for (var field in schema.m) if (schema.m.hasOwnProperty(field)) {
	    if (schema.m[field].is_indexed) {
		m[field] = schema.m[field].filter_parser();
	    }
	}
	/*
	// maybe this?	
	regtype(this.out, "_syncreq", SyncDB.Meteor.SyncReq,
		{ id : s, version : new serialization.Array(int), filter : new serialization.Struct(m) });
	 */
	var Or = new serialization.Or;
	Or.types = UTIL.values(m);
	regtype(this.out, "_syncreq", SyncDB.Meteor.SyncReq,
		{ id : s, version : version_type, filter : new serialization.Object(Or) });
	regtype(this.out, "_select", SyncDB.Meteor.Select,
		{ id : s, filter : SyncDB.Serialization.Filter });
	regtype(this.out, "_insert", SyncDB.Meteor.Insert,
		{ id : s, row : this.parser_out });
	// TODO: is an update allowed to change hidden fields?
	// insert is, so in principle this should be allowed
	regtype(this.out, "_update", SyncDB.Meteor.Update,
		{ id : s, row : this.parser_out, version : version_type,
		  key : schema.id.parser() });

	channel.set_cb(this.M(function(data) {
	    var a = this.atom_parser.parse(data);
	    for (var i = 0; i < a.length; i++) {
		var o;
		try {
		    o = this.incoming.decode(a[i]);
		} catch (err) {
		    UTIL.log("decoding %o failed: %o\n", a[i], err);
		    continue;
		}

		if (o instanceof SyncDB.Meteor.Sync) { // we dont care for id. just clean it up, man!
		    this.sync(o.rows);
		    if (!o.id) continue;
		}

		var f = this.requests[o.id];
		delete this.requests[o.id];

		if (f) {
		    if (o instanceof SyncDB.Meteor.Sync) { // reply to _insert
			if (o.rows.length != 1) UTIL.error("strange reply to _insert with multiple rows.");
			f(false, o.rows[0]);
		    } else if (a[i].type == "_error") {
			f(o.error);
		    } else {
			f(false, o.rows);
		    }
		} else if (!(o instanceof SyncDB.Meteor.Sync)) UTIL.log("could not find reply handler for %o(%o):%o\n", a[i].type, a[i], o);
	    }
	}));
    },
    // TODO: this is used to hook up local implicit drafts that might have results pending.
    register_request : function(id, callback) {
	this.requests[id] = callback;
    },
    send : function(o) {
	this.channel.send(this.out.encode(o).render());
    },
    low_select : function(filter, callback) {
	var id = UTIL.get_unique_key(5, this.requests);
	//UTIL.log("name: %o, value: %o\n", name, value);
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Select(id, filter));
	return id;
    },
    auto_set : function(row, f) {
	f(row);
    },
    low_update : function(row, callback, orow) {
	if (!orow || !orow.version) UTIL.error("need to specify old row for update.");
	if (!row.hasOwnProperty(this.schema.key)) UTIL.error("update needs it.");
	var id = UTIL.get_unique_key(5, this.requests);
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Update(id, row, orow.version, row[this.schema.key]));
	return id;
    },
    remove : function(name, type) {
	return this.M(function(value, row, callback) {
	    UTIL.error("METEOR REMOVE not supported, yet.");
	    var id = UTIL.get_unique_key(5, this.requests);
	    row[name] = value;
	    this.requests[id] = callback;
	    //this.send(new SyncDB.Meteor.Remove(id, row));
	    return id;
	});
    },
    low_insert : function(row, callback) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Insert(id, row));
	return id;
    },
    request_sync : function(version, filters) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.send(new SyncDB.Meteor.SyncReq(id, version, filters));
    }
});
SyncDB.LocalTable = SyncDB.Table.extend({
    constructor : function(name, schema, ls, db) {
	var done = 3;
	this.ls = ls || SyncDB.LS("");
	this.config = new SyncDB.TableConfig(this.ls, "_syncdb_"+name);
	this.base(name, schema, db);
	var eag = new UTIL.EventAggregator();
	var hashcheck = this.ready_ea.get_cb();
	this.config.get(eag.get_cb());
	eag.ready(this.M(function() {
	    if (this.config.schema().hashCode() != schema.hashCode()) {

		//this.ls.clear(this.M(function() {
		this.config.schema(schema);
		this.config.version(new SyncDB.Version(schema.m.version.n));
		hashcheck();
		//}));
		return;
	    }

	    this.config.schema(schema);
	    hashcheck();
	    //this.sync(this.config.version());
	}));
	this.I = {};
	for (var i = 0; i < schema.fields.length; i++) {
	    var type = schema.fields[i];
	    if (type.is_indexed || type.is_key) {
		var field = type.name;
		//UTIL.log("generating index for %s", field);
		this.I[field] = this.index("_syncdb_"+this.name+"_I"+field,
					   type, schema.m[schema.key]);
		SyncDB.LocalField.prototype.get.call(this.I[field], eag.get_cb());
		//UTIL.log("INDEX: %o", this.I[field]);
	    }
	}
	eag.start();
    },
    version : function() {
	return this.config.version();
    },
    clear : function(cb) {
	var c = 1;
	for (var key in this.I.value) {
	    c++;
	    this.db.remove(key, function () {
		if (!--c) cb(false);
	    });
	}

	--c;
	if (!c) cb(false);
    },
    is_permanent : function() {
	return this.ls.is_permanent;
    },
    index : function(name, field_type, key_name) {
	return field_type.get_index(this.ls, name, field_type, key_name);
    },
    remove : function(name, type) {
	var f = this.M(function(value, callback) {
	    var key = this.schema.key;
	    var k = type.get_key(this.name, key, value);

	    this.select_by(value, this.M(function(err, row) {
		if (err) return callback(err, row);
		for (var i in this.I) {
		    type.index_remove(this.I[i], row[i], row[key]);
		}

		this.ls.remove(k, this.M(function(error, value) { // TODO: make useful with different storage errors etc.
		    if (!error) {
			if (value) callback(false, row);
			else callback(new SyncDB.Error.NotFound());
		    } else callback(error);
		}));
	    }));
	});

	if (type.is_key) {
	    return f;
	} else {
	    throw("deleteing on !keys does not work yet.");
	}
    },
    low_select : function(filter, callback) {
	// probe the index and check sync.
	//
	// TODO: allow for partial results here. e.g. come up
	// with some mechanism to allow this index_lookup call
	// to return a partial results, which may in addition
	// contain another Filter and some results
	var ids;
	try {
	    ids = filter.index_lookup(this);
	} catch (error) {
	    callback(error);
	    //UTIL.call_later(callback, null, error);
	    return;
	}
	//UTIL.log("ids: %o\n", ids.join(","));
	if (ids.length) {
	    var id = this.schema.id;
	    var key = this.schema.key;

	    for (var i = 0; i < ids.length; i++) {
		ids[i] = id.get_key(this.name, key, ids[i]);
	    }

	    this.ls.get(ids, this.M(function(error, value) {
		//UTIL.log("LS returned %s -> %s", k, value);
		if (!error) {
		    for (var i = 0; i < value.length; i++)
			if (value[i])
			    value[i] = this.parser.decode(serialization.parse_atom(value[i]));
			else return callback(new SyncDB.Error.NotFound());
		    callback(false, value);
		} else callback(error);
	    }));
	} else return callback(false, []);
    },
    prune : function() { // delete everything
    },
    low_update : function(row, callback, orow, force) {
	var type = this.config.schema().id;
	var key = type.name;
	var f = this.M(function(error, row_) {
	    if (error) {
		callback(error);
		UTIL.log("%d: %o", row[key], error);
		UTIL.error("Some unexpected error occured. Sorry.");
	    }
	    if (!row_ || row_.length != 1) {
		UTIL.error("the id should be unique!");
	    }
	    row_ = row_[0];

	    if (row.version && row.version.lt(row_.version)) {
		callback(true, new SyncDB.Error.Collision(this, row, row_));
		return;
	    }

	    // TODO: check if row.version != orow.version && orow.version == row_.version.
	    // or rather do this more low level
	    var cb = this.M(function(error) {
		if (error) {
		    callback(new SyncDB.Error.Collision(this, row, row_));
		} else {
		    for (var i in this.I) if (this.I.hasOwnProperty(i)) {
			var t = this.schema.m[i];
			if (!t.eq(row_[i], row[i])) {
			    UTIL.log("changing %s from %o to %o", i, row_[i], row[i]);
			    t.index_remove(this.I[i], row_[i], row_[key]);
			    t.index_insert(this.I[i], row[i], row[key]);
			}
		    }
		    callback(false, row);
		}
	    });

	    if (force)
		this.ls.set(type.get_key(this.name, key, row[key]), this.parser.render(row), cb);
	    else
		this.ls.cas(type.get_key(this.name, key, row[key]),
			this.parser.render(row), this.parser.render(row_), cb);
	    
	    //for (var i in this.I) if (!row.hasOwnProperty(i)) error?

	});

	if (orow)
	    return f(false, [ orow ]);
	else this.select_by(row[key], f);
    },
    low_insert : function(row, callback, force) {
	var key = this.schema.key;

	var cb = this.M(function (error) {
	    //UTIL.log("stored in %o.", this.schema.id.get_key(this.name, key, row[key]));
	    if (error) return callback(error, row);
	    for (var i in this.I) {
		if (this.schema.m[i].is_unique || (this instanceof SyncDB.SyncedTable) || this.I[i].has(row[i]))
		    this.schema.m[i].index_insert(this.I[i], row[i], row[key]);
	    }
	    callback(false, row);
	});

	if (force) this.ls.set(this.schema.id.get_key(this.name, key, row[key]),
		    this.parser.encode(row).render(), cb);
	else this.ls.cas(this.schema.id.get_key(this.name, key, row[key]),
		    this.parser.encode(row).render(), undefined, cb);
    }
});
SyncDB.SyncedTableBase = SyncDB.LocalTable.extend({
    constructor : function() {
	this.base.apply(this, Array.prototype.slice.apply(arguments));
	this.callbacks = [];
	this._syncing = new UTIL.Event();
    },
    synced : function(cb) {
	if (this.callbacks)
	    this.callbacks.push(cb);
	else
	    UTIL.call_later(cb);
    },
    syncing : function(cb) {
	this._syncing.bind(UTIL.make_method(this, cb));
    },
    sync : function(rows) {
	// TODO: this should be triggered on completion of all updates, otherwise
	// something might fail and we still believe that we are up to date

	var version = this.version();
	var sync_ea = new UTIL.EventAggregator();
	sync_ea.progress(UTIL.make_method(this._syncing,
					  this._syncing.trigger));
	sync_ea.ready(this.M(function() {
	    for (var name in this.I) if (this.I.hasOwnProperty(name))
		this.I[name].autosync(true);
	    this.config.version(version);
	    SyncDB.Table.prototype.sync.call(this, rows);
	}));

	if (this.callbacks) {
	    sync_ea.ready(this.M(function(q) {
		for (var i = 0; i < q.length; i++) UTIL.call_later(q[i]);
	    }, this.callbacks));
	    delete this.callbacks;
	}

	for (var name in this.I) if (this.I.hasOwnProperty(name)) {
	    this.I[name].autosync(false);
	}

	for (var i = 0; i < rows.length; i++) {
	    var row = rows[i];
	    if (!row.hasOwnProperty(this.schema.key)) UTIL.error("error in row %o.", row);
	    // do this check in index locally
	    //
	    var cb = sync_ea.get_cb();
	    version = version.max(row.version);

	    if (this.I[this.schema.key].lookup_equal(row[this.schema.key]).length) {
		this.low_update(row, cb, undefined, true);
	    } else {
		this.low_insert(row, cb, true);
	    }
	}
	sync_ea.start();
    }
});
SyncDB.SyncedTable = SyncDB.SyncedTableBase.extend({
    constructor : function() {
	this.base.apply(this, Array.prototype.slice.apply(arguments));
	if (this.db) {
	    this.ready(this.M(function() {
		this.db.request_sync(this.version(), {});
	    }));
	}
    }
});
SyncDB.CachedTable = SyncDB.SyncedTableBase.extend({
    constructor : function() {
	this.base.apply(this, Array.prototype.slice.apply(arguments));
	if (this.db) {
	    this.ready(this.M(function() {
		var m = {};

		for (var field in this.I) if (this.I.hasOwnProperty(field)) {
		    m[field] = this.I[field].get_filter();
		}
		this.db.request_sync(this.version(), m);
	    }));
	}
    },
    low_select : function(filter, callback) {
	this.base(filter, this.M(function(error, rows) {
	    //UTIL.log("localtable says: %o, %o", error, rows);
	    if (error instanceof SyncDB.Error.NotFound)
		error = new SyncDB.Error.NoSync();
	    callback(error, rows);
	}));
    },
    index : function(name, field_type, key_name) {
	var index = field_type.get_index(this.ls, name, field_type, key_name);
	return index.extend(field_type.filter ? field_type.filter()
			    : SyncDB.FalseFilter);
    },
    update_index : function(filter, rows) {
	//UTIL.log("update_index(%o, %o)", filter, rows);
	// TODO: we want to support more complex queries here. also, the insert is async, so we
	// have to properly check return values, etc
	// also this insert should be handled by the filter itself. e.g. filters need to be able to
	// filter rows so that we can support Or. And would always be forbidden
	//
	// TODO: we might want to use all rows, even when they are then only accessible 
	rows = filter.index_insert(this, rows);

	for (var i = 0; i < rows.length; i++) {
	    this.low_insert(rows[i], function() {});
	}
    }
});
/** @namespace */
SyncDB.Flags = {
    Base : UTIL.Base.extend({
	toString : function() {
	    return "Base";
	}
    })
};
SyncDB.Flags.Unique = SyncDB.Flags.Base.extend({
    toString : function() {
	return "Unique";
    },
    is_unique : 1
});
SyncDB.Flags.Key = SyncDB.Flags.Unique.extend({
    toString : function() {
	return "Key";
    },
    is_indexed : 1,
    is_key : 1
});
SyncDB.Flags.Index = SyncDB.Flags.Base.extend({
    toString : function() {
	return "Index";
    },
    is_indexed : 1
});
SyncDB.Flags.Mandatory = SyncDB.Flags.Base.extend({
    toString : function() {
	return "Mandatory";
    },
    is_mandatory : 1
});
SyncDB.Flags.WriteOnly = SyncDB.Flags.Base.extend({
    toString : function() {
	return "WriteOnly";
    },
    is_readable : 1
});
SyncDB.Flags.ReadOnly = SyncDB.Flags.Base.extend({
    toString : function() {
	return "ReadOnly";
    },
    is_writable : 0
});
SyncDB.Flags.Hashed = SyncDB.Flags.Base.extend({
    toString : function() {
	return "Hashed";
    },
    transform : function(f) {
	return function(data) {
	    return (new UTIL.SHA256.Hash()).update(f(data)).hex_digest();
	}
    }
});
SyncDB.Flags.Automatic = SyncDB.Flags.Base.extend({
    is_automatic: 1
});
SyncDB.Flags.AutoIncrement = SyncDB.Flags.Automatic.extend({
    get_val : function (db, name, type, cb) {
	var n = "_syncdb_CNT_" + db.name + "_" + name;
	if (!SyncDB.Flags.AutoCache[n])
	    SyncDB.Flags.AutoCache[n] = new SyncDB.LocalField(n, type.parser(), 1);

	var field = SyncDB.Flags.AutoCache[n];
	field.get(function(val) {
		    field.set(type.increment(val));
		    //UTIL.log("INCREMENT %o", val);
		    cb(val);
		  });
    }
});
SyncDB.Flags.AutoCache = {};
SyncDB.Serialization.Flag = serialization.generate_structs({
    _automatic : SyncDB.Flags.Automatic,
    //_hash : SyncDB.Flags.Hash,
    _index : SyncDB.Flags.Index,
    _key : SyncDB.Flags.Key,
    _mandatory : SyncDB.Flags.Mandatory,
    _readonly : SyncDB.Flags.ReadOnly,
    _unique : SyncDB.Flags.Unique,
    _writeonly : SyncDB.Flags.WriteOnly
});
/** @namespace */
SyncDB.Types = {
    // certain types are not really indexable, so this might be split
    Base : UTIL.Base.extend({
	eq : function (a, b) {
	    return a == b; 
	},
	_types : {
	    name : new serialization.Method(),
	    flags : new serialization.Array(SyncDB.Serialization.Flag),
	},
	constructor : function(name) {
	    this.name = name;
	    this.flags = Array.prototype.slice.call(arguments, 1);
	    //UTIL.log("creating %s with %d arguments.\n", this.toString(), arguments.length);
	    // BACKWARDS loop for things that are combined recursively
	    for (var i = this.flags.length-1; i >= 0; i--) {
		for (var name in this.flags[i]) if (UTIL.functionp(this.flags[i][name])) {
		    switch (name) {
		    case "check":
		    case "transform":
			// these functions are expected to curry
			this[name] = this.flags[i][name](this[name]);
			break;
		    }
		}
	    }
	    // FORWARD loop for priorities
	    for (var i = 0; i < this.flags.length; i++) {
		//UTIL.log("scanning %o\n", this.flags[i]);
		for (var sym in this.flags[i]) {
		    //UTIL.log(name);
		    if (UTIL.has_prefix(sym, "is_")
			|| UTIL.has_prefix(sym, "get_")) {
			if (!this.hasOwnProperty(sym))
			    this[sym] = this.flags[i][sym];
		    }
		}
	    }
	    if (!this.hasOwnProperty("is_writable")) this.is_writable = true;
	    if (!this.hasOwnProperty("is_readable")) this.is_readable = true;
	    this.parser = UTIL.cached(this.parser);
	    this.filter_parser = UTIL.cached(this.filter_parser);
	},
	get_key : function() {
	    return Array.prototype.slice.apply(arguments).join("_");
	},
	get_index : function(ls, name, field_type, key_name) {
	    /*
	    UTIL.log("will trace 'get_index3'");
	    UTIL.error("get_index3(%o, %o)", name, key_name);
	    */
	    return new SyncDB.Index(ls, name, this, key_name);
	},
	index_lookup : function(index, key) {
	    if (UTIL.objectp(key) && key.index_lookup) {
		return key.index_lookup(index);
	    } else return index.get(key);
	},
	fromString : function(s) {
	     return s;
	},
	index_insert : function(index, key, id) {
	    return index.insert(key, id);
	},
	index_remove : function(index, key, id) {
	    return index.remove(key, id);
	},
	filter_parser : function() {
	    return new serialization.Bloom(this.hash());
	},
	Equal : function(value) {
	    return new SyncDB.Filter.Equal(this.name, this.parser(), value);
	},
	True : function() {
	    return new SyncDB.Filter.True(this.name);
	},
	Bloom : function(filter) {
	    return new SyncDB.Filter.Bloom(this.name,
					   this.filter_parser(), filter);
	}
    })
};
SyncDB.Types.Filterable = SyncDB.Types.Base.extend({
    bloom_size: function() {
	return 32;
    },
    bloom_probability : function() {
	return 0.001;
    },
    filter : function() {
	return SyncDB.BloomFilter;
    }
});
SyncDB.Types.Integer = SyncDB.Types.Filterable.extend({
    parser : function() {
	return serialization.integer;
    },
    random : function() {
	return Math.floor(0xffffffff*Math.random());
    },
    toString : function() { return "Integer"; },
    increment : function(old) {
	return old + 1;
    },
    hash : function() {
	return new UTIL.Int.Hash();
    },
    get_index : function(ls, name, field_type, key_name) {
	return this.base(ls, name, field_type, key_name)
		    .extend( (this.is_unique || this.is_key)
			     ? SyncDB.MappingIndex
			     : SyncDB.MultiIndex);
    /*
	if (this.is_unique || this.is_key)
	    return new SyncDB.CritBitIndex(ls, name, this, key_name);
	else //if (this.is_indexed)
	    //return new SyncDB.MultiIndex(ls, name, this, key_name);
	    return this.base.apply(this, Array.prototype.slice.call(arguments));
    */
    },
    fromString : function (s) {
	return parseInt(s);
    },
    Lt : function(val) {
	return new SyncDB.Filter.Lt(this.name, this.parser(), val);
    },
    Le : function(val) {
	return new SyncDB.Filter.Le(this.name, this.parser(), val);
    },
    Gt : function(val) {
	return new SyncDB.Filter.Gt(this.name, this.parser(), val);
    },
    Ge : function(val) {
	return new SyncDB.Filter.Ge(this.name, this.parser(), val);
    },
    Contains : function(range) {
	return new SyncDB.Filter.Contains(this.name, new serialization.Range(this.parser()), range);
    }
});
SyncDB.Types.String = SyncDB.Types.Filterable.extend({
    parser : function() {
	return serialization.string;
    },
    random : function() {
	return UTIL.get_random_key(10);
    },
    toString : function() { return "String"; },
    get_index : function(ls, name, field_type, key_name) {
	return this.base(ls, name, field_type, key_name)
		    .extend( (this.is_unique || this.is_key)
			     ? SyncDB.MappingIndex
			     : SyncDB.MultiIndex);
    },
    hash : function() {
	return new UTIL.SHA256.Hash();
    }
});
SyncDB.Types.Image = SyncDB.Types.Base.extend({
    parser : function() {
	return serialization.image;
    },
    toString : function() { return "Image"; }
});
SyncDB.Types.Vector = SyncDB.Types.Base.extend({
    // ideally this would somehow use inheritance, but I like
    // the idea that its inside the prototype with easy lookup
    _types : {
	name : new serialization.Method(),
	flags : new serialization.Array(SyncDB.Serialization.Flag),
	types : function(p) {
	    return new serialization.Array(p);
	}
    },
    eq : function(a, b) {
	if (UTIL.arrayp(a) && UTIL.arrayp(b)) {
	    if (a.length != b.length) return false;
	    for (var i = 0; i < a.length; i++)
		if (!this.types[i].eq(a[i], b[i])) return false;
	    return true;
	}
	return false;
    },
    toString : function() { return "Vector"; },
    constructor : function(name, types) {
	this.types = types;
	this.base.apply(this, [ name ].concat(Array.prototype.slice.call(arguments, 2)));
    },
    parser : function(type, constructor, tuple) {
	var l = new Array(this.types.length+2);
	l[0] = type||"_vector";
	l[1] = constructor||false;
	for (var i = 0; i < l.length-2; i++)
	    l[i+2] = this.types[i].parser();
	return UTIL.create(tuple||serialization.Tuple, l);
    }
});
SyncDB.Types.Range = SyncDB.Types.Vector.extend({
    toString : function() { return "Range"; },
    constructor : function(name, from, to) {
	this.base.apply(this, [ name, [ from, to ] ].concat(Array.prototype.slice.call(arguments, 3)));
    },
    parser : function(type) {
	return new serialization.Range(this.types[0].parser(), type);
    },
    get_index : function(ls, name, field_type, key_name) {
	if (this.is_unique || this.is_key)
	    UTIL.error("Ranges cannot be unique (yet).");
	else //if (this.is_indexed)
	    return this.base(ls, name, this, key_name).extend(SyncDB.RangeIndex);
    },
    filter_parser : function() {
	return new serialization.RangeSet(this.parser())
    },
    filter : function() {
	return SyncDB.RangeFilter;
    },
    //RangeSet : // TODO
    Overlaps : function(range) {
	return new SyncDB.Filter.Overlaps(this.name, this.parser(), range);
    },
    eq : function(a, b) {
	if (a instanceof CritBit.Range && b instanceof CritBit.Range) {
	    return (this.types[0].eq(a.a, b.a) && this.types[1].eq(a.b, b.b));
	}
	return false;
    }
});
SyncDB.Types.Version = SyncDB.Types.Vector.extend({
    toString : function() { return "Version"; },
    _types : {
	name : new serialization.Method(),
	flags : new serialization.Array(SyncDB.Serialization.Flag),
	types : function(p) {
	    return new serialization.Array(p);
	},
	n : new serialization.Integer()
    },
    constructor : function(name, n) {
	var t = new Array(n);

	this.n = n;
	for (var i = 0; i < n; i++) t[i] = new SyncDB.Types.Integer("_version_"+i);
	this.base.apply(this, [ name, t ].concat(Array.prototype.slice.call(arguments, 2)));
    },
    parser : function(type, constructor) {
	return this.base(type||"_version", undefined, serialization.Tuple.extend({
	    generate_encode : function(o, type, data) {
		return this.base(o.Index("a"), type, data);
	    },
	    generate_decode : function(type, data, ret) {
		var b = new lambda.Block();
		b.add(this.base(type, data, ret));
		b.add(ret.Set(new lambda.Template("new SyncDB.Version(%%)", ret)));
		return b;
	    },
	    generate_can_encode : function(o, ret) {
		return ret.Set(new lambda.Template("%% instanceof SyncDB.Version", o));
	    }
	}));
    },
    eq : function(a, b) {
	return a.eq(b);
    }
});
SyncDB.Date = function() {
    Date.apply(this, Array.prototype.slice.call(arguments));
    this.sizeof = function() {
	return CritBit.Size(0,32);
    };
    this.count_prefix = function(d1) {
	var a = Math.floor(this.getTime()/1000);
	var b = Math.floor(d1.getTime()/1000);
	return new CritBit.Size(0, 32 - CritBit.clz(a ^ b));
    };
    this.get_bit = function(size) {
	return CritBit.get_bit(Math.floor(this.getTime()/1000), size);
    };
};
SyncDB.Date.prototype = Date.prototype;
SyncDB.Types.Date = SyncDB.Types.Base.extend({
    toString : function() { return "Date"; },
    parser : function() {
	return serialization.date;
    },
    eq : function(a, b) {
	return a.getTime() == b.getTime();
    }
});
SyncDB.Types.Array = SyncDB.Types.Base.extend({
    constructor : function(name, type) {
	this.type = type;
	this.base.apply(this, [ name ].concat(Array.prototype.slice.call(arguments, 2)));
	if (this.is_unique) SyncDB.error("Arrays cannot be unique, retard!");
	if (type instanceof SyncDB.Types.Array)
	    SyncDB.error("nested arrays are not implemented, yet. we want food!");
    },
    parser : function() {
	return new serialization.Array(this.type.parser());
    },
    toString : function() { return "Array"; },
    get_index : function(ls, name, field_type, key_name) {
	return this.type.get_index(ls, name, field_type, key_name);
    },
    index_insert : function(index, key, id) {
	for (var i = 0; i < key.length; i++)
	    index.insert(key[i], id);
    },
    index_lookup : function(index, key) {
	if (UTIL.arrayp(key)) {
	    return UTIL.create(SyncDB.Filter.And, key);
	} else if (UTIL.objectp(key) && key.index_lookup) {
	    // complex types, e.g. AND, OR and shit like that
	    return key.index_lookup(index);
	} else return index.get(key);
    },
    random : function() {
	var a = new Array(20);
	for (var i = 0; i < 20; i++)
	    a[i] = this.type.random();
	return a;
    },
    index_remove : function(index, key, id) {
	if (UTIL.arrayp(key)) {
	    for (var i = 0; i < key.length; i++)
		index.remove(key[i], id);
	} else index.remove(key, id);
    }
});
SyncDB.Serialization.Type = serialization.generate_structs({
    _string : SyncDB.Types.String,
    _integer : SyncDB.Types.Integer,
    _range : SyncDB.Types.Range,
    _vector : SyncDB.Types.Vector,
    _date : SyncDB.Types.Date,
    _image : SyncDB.Types.Image,
    _version : SyncDB.Types.Version
});
SyncDB.Serialization.Schema = serialization.Array.extend({
    constructor : function() {
	this.type = "_schema";
	this.base(SyncDB.Serialization.Type);
    },
    generate_encode : function(o, type, data) {
	var b = new lambda.Block(data.scope);
	b.add(o.Set(o.Index("fields")));
	b.add(this.base(o, type, data));
	return b;
    },
    generate_decode : function(type, data, ret) {
	var b = new lambda.Block(ret.scope);
	b.add(this.base(type, data, ret));
	b.add(ret.Set(new lambda.Template("UTIL.create(SyncDB.Schema, %%)", ret)));
	return b;
    },
    generate_can_encode : function(o, ret) {
	return ret.Set(new lambda.Template("%% instanceof SyncDB.Schema", o));
    }
});
SyncDB.DraftTable = SyncDB.LocalTable.extend({
    constructor : function(name, schema, ls, db) {
	// add some extra fields to the schema
	// we also need to support deletes?
	// insert/update
	//
	// we disallow chaining here. its really not supposed to work
	if (db) UTIL.error("Chaining not allowed with Draft tables.");
	this.base(name, schema, ls);
	this.draft_index = (new SyncDB.Index(this.ls, "_syncdb_DI_" + this.name, schema.m[schema.key], schema.m[schema.key]))
			    .extend(SyncDB.MappingIndex);
    },
    insert : function(row, cb) {
	row[this.schema.key] = 0;
	return this.create_draft(row, cb);
	this.base(row, cb);
    },
    create_draft : function(row, cb) {
	SyncDB.LocalTable.prototype.insert.call(this, row, this.M(function(err, row_) {
	    if (err) return cb(err, row_);
	    this.draft_index.insert(row_[this.schema.key],
				 row [this.schema.key]);
	    cb(err, row_);
	}));
    }
});
SyncDB.Connector = SyncDB.LocalField.extend({
    constructor : function(ls, drafts, online, cb) {
	this.drafts = drafts;
	this.online = online;
	this.cb = cb;
	this.base(ls, "_syncdb_connector_"+drafts.name+"_"+online.name,
		  new serialization.Object(drafts.schema.m[drafts.schema.key].parser()),
		  { });
	this.get(this.M(function () {
	    for (var key in this.value)
		this.commit(key);
	    // retrigger all commits
	}));
    },
    commit_all : function() {
	for (var x in this.drafts.draft_index.value) this.commit(x);
    },
    commit : function(key) {
	// commit key from draft table online
	this.drafts.select_by(key, this.M(function(error, row) {
	    if (error) {
		return this.cb(key, error);
	    }
	    var callback = this.M(function(error, row) {
		delete this.value[key];
		this.sync();
		if (!error) {
		    this.drafts.draft_index.remove(key, oid);
		    this.drafts.remove_by(key,
			this.M(function(err, _row) {
			    if (err) UTIL.log("Something fishy happeneed in Connector#commit: %o", err);
			    this.cb(key, error, row);
			}));
		} else
		    this.cb(key, error, row);
	    });
	    this.value[key] = 1;
	    this.sync();
	    var oid = this.drafts.draft_index.get(key);
	    if (oid.length) { // corresponds to online entry
		oid = oid[0];
		// update or delete
		if (!row) this.online.remove_by(oid, callback);
		else this.online.update(oid, row, callback);
	    } else this.online.insert(row, callback);
	}));
    }
});
