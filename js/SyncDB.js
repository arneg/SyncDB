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
	})
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
    constructor : function(field) {
	this.field = field;
	this.args = Array.prototype.slice.apply(arguments);
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
	value : new serialization.Any()
    },
    constructor : function(field, value) {
	this.field = field;
	this.value = value;
    },
    index_lookup : function(table) {
	if (table.I[this.field]) {
	    var type = table.schema.m[this.field];
	    var v = type.parser().decode(this.value);
	    return table.I[this.field].get(v);
	} else UTIL.error("no index for %s", this.field);
    },
    filter : function(schema, rows) {
	var type = schema.m[this.field];
	var v = type.parser().decode(this.value);
	var ret = [];
	for (var i = 0; i < rows.length; i++)
	    if (rows[i][this.field] == v) ret.push(rows[i]);
	return ret;
    },
    index_insert : function(table, rows) {
	var index;
	if (index = table.I[this.field]) {
	    var type = table.schema.m[this.field];
	    for (var i = 0; i < rows.length; i++) {
		index.set(rows[i][this.field], rows[i][table.schema.key]);
	    }
	    if (index.set_filter) {
		var v = type.parser().decode(this.value);
		index.set_filter(v);
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
	var type = table.schema.m[this.field];
	var index = table.I[this.field];
	if (!index || !index.overlaps)
	    throw(new SyncDB.Error.NotFound());
	return index.overlaps(type.parser().decode(this.value));
    }
});
SyncDB.Filter.Gt = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var type = table.schema.m[this.field];
	var index = table.I[this.field];
	if (!index || !index.lookup_gt)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_gt(type.parser().decode(this.value));
    }
});
SyncDB.Filter.Ge = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var type = table.schema.m[this.field];
	var index = table.I[this.field];
	if (!index || !index.lookup_ge)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_ge(type.parser().decode(this.value));
    }
});
SyncDB.Filter.Lt = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var type = table.schema.m[this.field];
	var index = table.I[this.field];
	if (!index || !index.lookup_lt)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_lt(type.parser().decode(this.value));
    }
});
SyncDB.Filter.Le = SyncDB.Filter.Equal.extend({
    index_lookup : function(table) {
	var type = table.schema.m[this.field];
	var index = table.I[this.field];
	if (!index || !index.lookup_le)
	    throw new SyncDB.Error.NotFound();
	return index.lookup_le(type.parser().decode(this.value));
    }
});
/** @namespace */
SyncDB.Serialization = {};
SyncDB.Serialization.Filter = serialization.generate_structs({
    _or : SyncDB.Filter.Or,
    _and : SyncDB.Filter.And,
    _equal : SyncDB.Filter.Equal,
    _true : SyncDB.Filter.True,
    _false : SyncDB.Filter.False,
    _overlaps : SyncDB.Filter.Overlaps,
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
	cb(false, value);
	//UTIL.call_later(cb, null, false, value);
    },
    get : function(key, cb) {
	if (UTIL.arrayp(key)) {
	    for (var i = 0; i < key.length; i++) key[i] = this.m[key[i]];
	} else key = this.m[key];
	//cb(false, key);
	UTIL.call_later(cb, null, false, key);
    },
    remove : function(key, cb) {
	var v = this.m[key];
	delete this.m[key];
	cb(false, v);
	//UTIL.call_later(cb, null, false, v);
    },
    clear : function(cb) {
	this.m = {};
	cb(false);
	//UTIL.call_later(cb, null, false);
    },
    toString : function() {
	return "SyncDB.KeyValueMapping";
    }
});
if (UTIL.App.has_local_storage) {
    SyncDB.KeyValueStorage = UTIL.Base.extend({
	set : function(key, value, cb) {
	    try {
		localStorage[key] = value;
		//cb(false, value);
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		cb(err);
		//UTIL.call_later(cb, null, err);
	    }
	},
	is_permanent : true,
	get : function(key, cb) {
	    try {
		if (UTIL.arrayp(key)) {
		    for (var i = 0; i < key.length; i++)
			key[i] = localStorage[key[i]];
		} else 
		    key = localStorage[key];
		//cb(false, value);
		UTIL.call_later(cb, null, false, key);
	    } catch(err) {
		cb(err);
		//UTIL.call_later(cb, null, err);
	    }

	},
	remove : function(key, cb) {
	    try {
		var value = localStorage[key];
		delete localStorage[key];
		//cb(false, value);
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		cb(err);
		//UTIL.call_later(cb, null, err);
	    }
	},
	clear : function(cb) {
	    // TODO: remove only sync_db entries. ;_)
	    localStorage.clear();
	    cb(false);
	    //UTIL.call_later(cb, null, false);
	},
	toString : function() {
	    return "SyncDB.KeyValueStorage";
	}
    });
}
if (UTIL.App.is_ipad || UTIL.App.is_phone || UTIL.App.has_local_database) {
    SyncDB.KeyValueDatabase = UTIL.Base.extend({
	constructor : function(cb) {
		this.db = openDatabase("SyncDB", "1.0", "SyncDB", 5*1024*1024);
		this.init(cb);
	},
	init : function(cb) {
		try {
		    this.db.transaction(this.M(function (tx) {
			try {
			    tx.executeSql("CREATE TABLE IF NOT EXISTS sLsA (key VARCHAR(255) PRIMARY KEY, value BLOB);", [],
					  this.M(function(tx, data) {
					    this.M(cb)(false);
					  }),
					  this.M(function(tx, err) {
					    this.M(cb)(err);
					  }));
			} catch(err) {
			    this.M(cb)(err);
			}
		    }));
		} catch (err) {
		    this.M(cb)(err);
		}
	},
	is_permanent : true,
	q : [],
	get : function(key, cb) {
	    if (this.q) {
		this.q.push(function() { this.get(key, cb); });
	    } else {
		cb = UTIL.safe(cb);
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    var i, arr = UTIL.arrayp(key);
		    var query = "SELECT * FROM sLsA WHERE ";
		    if (arr) {
			var t = new Array(key.length);
			for (i = 0; i < t.length; i++) t[i] = "key=?";
			query += t.join(" OR ") + ";";
		    } else query += "key=?;";

		    tx.executeSql(query, arr ? key : [key], this.M(function(tx, data) {
			var m = {};
			for (i = 0; i < data.rows.length; i++) m[data.rows.item(i).key] = this.decode(data.rows.item(i).value);
			if (arr) {
			    for (i = 0; i < key.length; i++) key[i] = m[key[i]];
			} else key = m[key];
			cb(false, key);
			this.replay();
		    }), this.M(function(tx, err) {
			cb(err);
			this.replay();
		    }));
		}));
	    }
	},
	set : function(key, val, cb) {
	    if (this.q) {
		this.q.push(function() { this.set(key, val, cb); });
	    } else {
		cb = UTIL.safe(cb);
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    tx.executeSql("INSERT OR REPLACE INTO sLsA (key, value) VALUES(?, ?);", [ key, this.encode(val) ],
				  this.M(function(tx, data) {
				    if (data.rowsAffected != 1) cb(true);
				    else cb(false, val);
				    this.replay();
				  }),
				  this.M(function (tx, err) {
				    cb(err);
				    this.replay();
				  }));
		}));
	    }
	},
	remove : function(key, cb) {
	    if (this.q) {
		this.q.push(function() { this.remove(key, cb); });
	    } else {
		cb = UTIL.safe(cb);
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    tx.executeSql("SELECT * FROM sLsA WHERE key=?;", [key],
			this.M(function(tx, data) {
			    tx.executeSql("DELETE FROM sLsA WHERE key=?;", [key],
					  this.M(function (tx) {
					      cb(false, this.decode(data.rows.item(0).value));
					      this.replay();
					  }),
					  this.M(function (tx, err) {
					      cb(err);
					      this.replay();
					  }));
					  }),
			this.M(function (tx, err) {
			    cb(err);
			    this.replay();
			}));
		}));
	    }
	},
	encode : function(s) {
	    return UTF8.encode(s.replace(/\0/g, "\u0100"));
	},
	decode : function(s) {
	    return UTF8.decode(s).replace(/\u0100/g, "\0");
	},
	replay : function() {
	    var q = this.q;
	    this.q = undefined;
	    for (var i = 0; i < q.length; i++) {
		q[i].apply(this);
	    }
	},
	clear : function(cb) {
	    cb = UTIL.safe(cb);
	    this.db.transaction(this.M(function (tx) {
		tx.executeSql("DROP TABLE sLsA;", [], this.M(function() {
			      this.init(cb);
			  }), function(err) {
			      cb(err);
			  });
	    }));
	},
	toString : function() {
	    return "SyncDB.KeyValueDatabase";
	}
    });
    try {
	SyncDB.LS = new SyncDB.KeyValueDatabase(function (err) {
	    //UTIL.log("%o", err);
	    if (err) {
		SyncDB.LS = new (SyncDB.KeyValueStorage || SyncDB.KeyValueMapping)();
	    }
	    var q = this.q;
	    this.q = undefined;
	    for (var i = 0; i < q.length; i++) {
	       q[i].apply(SyncDB.LS);
	    }
	});
    } catch (err) { }
}
if (!SyncDB.LS) SyncDB.LS = new(SyncDB.KeyValueStorage || SyncDB.KeyValueMapping)();
SyncDB.LocalField = UTIL.Base.extend({
    constructor : function(name, parser, def) {
	this.name = name;
	this.parser = parser;
	this.value = undefined;
	this.def = def;
	this.will_set = false;
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
	    SyncDB.LS.get(this.name, this.M(function(err, value) {
		var ret;
		if (err) {
		    ret = undefined;
		} else {
		    if (UTIL.stringp(value))
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
    sync : function() {
	// We want to allow for looping over a repeated set call (e.g. MultiIndex)
	if (this.will_set) return;
	this.will_set = true;

	UTIL.call_later(function() {
		this.will_set = false;
		if (this.value == undefined) {
		    SyncDB.LS.remove(this.name, function () {});
		} else {
		    SyncDB.LS.set(this.name, this.parser.encode(this.value).render(), function () {});
		}
	    }, this);
    }
});
SyncDB.RangeIndex = SyncDB.LocalField.extend({
    constructor : function(name, tkey, tid) {
	this.tkey = tkey;
	this.tid= tid;
	this.base(name, new serialization.Struct("_range_index", {
	    m : new serialization.MultiRangeSet(tkey.parser(tid.parser())),
	    filter : tkey.filter_parser()
	}), {
	    m : new CritBit.MultiRangeSet(),
	    filter : tkey.filter()
	});
    },
    set : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	index.value = id;
	this.value.m.insert(index);
	this.value.filter.insert(index);
	this.sync();
    },
    set_filter : function(range) {
	this.value.filter.insert(range);
	this.sync();
    },
    get_filter : function() {
	return this.value.filter;
    },
    has : function(index) {
	return this.value.filter.contains(index);
    },
    overlaps : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) throw(new SyncDB.Error.NotFound());
	var a = this.value.m.overlaps(index);
	if (!a.length) return [];
	var ret = new Array(a.length);
	for (var i = 0; i < a.length; i ++) {
	    ret[i] = a[i].value;
	}
	return ret;
    },
    get : function(index) {
	return this.overlaps(index);
    },
    remove : function(index) {
	UTIL.error("remove not yet supported here!");
    }
});
// TODO: this should really be a subindex that doesnt do lookup_get, only
// range lookups and has a RangeSet for the cached entries.
SyncDB.CritBitIndex = SyncDB.LocalField.extend({
    constructor : function(name, tkey, tid) {
	this.tkey = tkey;
	this.type = tid;
	this.base(name, new serialization.Struct("_index", {
		filter : tkey.filter_parser(),
		m : new serialization.CritBit(tkey.parser(), tid.parser()), 
		removed : new serialization.Integer()
	    }), {
		m : new CritBit.Tree(),
		filter : tkey.filter(),
		removed : 0
	});
    },
    set : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	this.value.m[index] = id;
	this.value.m.insert(index, id);
	if (this.value.filter.set(index))
	    this.regen_filter();
	// adding something can be done cheaply, by appending the tuple
	this.sync();
    },
    get : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) throw(new SyncDB.Error.NotFound());
	return [ this.value.m.index(index) ];
    },
    has : function(index) {
	return this.value.m.index(index) !== null;
    },
    regen_filter : function() {
	// decide when to regenerate here. Lets say, we 
	var p = this.value.filter.prob();
	if (p > 0.002) {
	    UTIL.log("probability is bad: %f. regenerating filter.", p);
	    var n = this.value.m.length();
	    var filter = new UTIL.Bloom.Filter(n, 0.001, this.tkey.hash());
	    this.value.m.foreach(function(key, v) {
		filter.set(key);
	    });
	    UTIL.log("new probability is %f. n: %d, m: %d", filter.prob(), filter.n, filter.table_mag);
	    this.value.filter = filter;
	    this.sync();
	} else UTIL.log("probability is still good enough: %f", p);
    },
    remove : function(index, value) {
	// keep track of deletes, so we know how bad our filter got.
	// regenerate as needed.
	this.value.m.remove(index);
	if (this.value.filter.remove(index)) {
	    this.regen_filter();
	    this.sync();
	}
    },
    get_filter : function() {
	return this.value.filter;
    },
    toString : function() {
	return "MappingIndex("+this.name+","+this.type+")";
    },
    values : function() {
	return this.value.m.values();
    },
    lookup_lt : function(val) {
	var r = [];
	var node = this.value.m.lt(val);
	while (node && node.value < val) {
	    r.push(node.value);
	    node = node.backward();
	}
	if (!r.length) throw(new SyncDB.Error.NotFound());
	return r;
    },
    lookup_le : function(val) {
	var r = [];
	var node = this.value.m.le(val);
	while (node && node.value <= val) {
	    r.push(node.value);
	    node = node.backward();
	}
	if (!r.length) throw(new SyncDB.Error.NotFound());
	return r;
    },
    lookup_gt : function(val) {
	var r = [];
	var node = this.value.m.gt(val);
	while (node && node.value > val) {
	    r.push(node.value);
	    node = node.forward();
	}
	if (!r.length) throw(new SyncDB.Error.NotFound());
	return r;
    },
    lookup_ge : function(val) {
	var r = [];
	var node = this.value.m.ge(val);
	while (node && node.value >= val) {
	    r.push(node.value);
	    node = node.forward();
	}
	if (!r.length) throw(new SyncDB.Error.NotFound());
	return r;
    }
});
SyncDB.MappingIndex = SyncDB.LocalField.extend({
    constructor : function(name, tkey, tid) {
	this.tkey = tkey;
	this.type = tid;
	this.base(name, new serialization.Struct("_index", {
		filter : tkey.filter_parser(),
		m : new serialization.Object(tid.parser()), 
		removed : new serialization.Integer()
	    }), {
		m : {},
		//filter : new UTIL.Bloom.Filter(32, 0.001, tkey.hash()),
		filter : tkey.filter(),
		removed : 0
	});
    },
    set : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	this.value.m[index] = id;
	if (this.value.filter.set(index))
	    this.regen_filter();
	// adding something can be done cheaply, by appending the tuple
	this.sync();
    },
    get : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) throw(new SyncDB.Error.NotFound());
	return [ this.value.m[index] ];
    },
    has : function(index) {
	return this.value.m.hasOwnProperty(index);
    },
    regen_filter : function() {
	// decide when to regenerate here. Lets say, we 
	var p = this.value.filter.prob();
	if (p > 0.002) {
	    var n = UTIL.keys(this.value.m).length;
	    var filter = new UTIL.Bloom.Filter(n, 0.001, this.tkey.hash());
	    for (var key in this.value.m)
		if (this.value.m.hasOwnProperty(key))
		    filter.set(key);
	    this.value.filter = filter;
	    this.sync();
	}
    },
    remove : function(index, value) {
	// keep track of deletes, so we know how bad our filter got.
	// regenerate as needed.
	delete this.value.m[index];
	if (this.value.filter.remove(index)) {
	    this.regen_filter();
	    this.sync();
	}
    },
    get_filter : function() {
	return this.value.filter;
    },
    toString : function() {
	return "MappingIndex("+this.name+","+this.type+")";
    },
    values : function() {
	return UTIL.values(this.value.m);
    }
});
SyncDB.MultiIndex = SyncDB.LocalField.extend({
    constructor : function(name, type, id) {
	this.type = type;
	this.id = id;
	//UTIL.log("%o %o %o", name, type, id);
	this.base(name, new serialization.Object(new serialization.SimpleSet()), {});
    },
    set : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) {
	    this.value[index] = { };
	}
	this.value[index][id] = 1;
	// adding something can be done cheaply, by appending the tuple
	//UTIL.log(">> %o", this.value);
	this.sync();
    },
    get : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.has(index)) throw(new SyncDB.Error.NotFound());
	return UTIL.keys(this.value[index]);
    },
    has : function(index) {
	return this.value.hasOwnProperty(index);
    },
    remove : function(index, value) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (this.has(index)) {
	    delete this.value[index][value];
	    if (!this.value[index].length) delete this.value[index];
	    this.sync();
	}
    },
    toString : function() {
	return "MultiIndex("+this.name+","+this.type+")";
    }
});
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
    },
    hashCode : function() {
	return (new UTIL.SHA256.Hash()).update(this.schema_parser().encode(this).render()).hex_digest();
    },
    schema_parser : function() {
	return new SyncDB.Serialization.Schema();
    },
    parser : function(filter) {
	var n = {};
	for (var name in this.m) if (this.m.hasOwnProperty(name)) {
	    if (!filter || filter(name, this.m[name]))
		n[name] = new serialization.Or(new serialization.False(),
					       this.m[name].parser());
	}
	return new serialization.Struct("_schema", n);
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
    constructor : function(name) {
	// create self updating field with given serialization for
	// the schema and version, etc.
	this.base(name, new serialization.Struct(null, {
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
	    this.value.version = arguments[0];
	    this.sync();
	}
	return this.value.version;
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
	this.name = name;
	this.schema = schema;
	this.parser = schema.parser();
	this.parser_in = schema.parser(function(name, type) {
	    return !type.is_hidden;
	});
	this.parser_out = schema.parser(function(name, type) {
	    return !type.is_hidden && !type.is_automatic;
	});
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
    get_version : function() {},
    // I am not sure what this thing was supposed to do.
    sync : function(version) {},
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
	var f = this.M(function (error, n) {
	    // TODO: if this was lost, we loose sync.
	    if (!error) try { 
		    this.low_insert(n, callback);
		} catch (e) {
		    callback(e, n);
		}
	    else callback(error, row);
	});

	if (this.db) {
	    this.db.insert(row, f);
	} else {
	    this.auto_set(row, function(n) {
		f(false, n);
	    });
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
    update : function(id, row, callback, orow) {
	if (this.db) {
	    this.db.update(id, row, this.M(function(error, row_) {
		if (error) {
		    callback(error, row_);
		} else this.low_update(id, row_, callback, orow);
	    }));
	} else return this.low_update(id, row, callback, orow);
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
    })
};
SyncDB.Meteor.Update = SyncDB.Meteor.Base.extend({});
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
	var int = new serialization.Integer();
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
		{ id : s, version : new serialization.Array(int), rows : new serialization.Array(this.parser_in) });
	this.out = new serialization.Polymorphic();
	var m = {};
	for (var field in schema.m) if (schema.m.hasOwnProperty(field)) {
	    if (schema.m[field].is_indexed) {
		m[field] = schema.m[field].filter_parser();
	    }
	}
	var Or = new serialization.Or;
	Or.types = UTIL.values(m);
	regtype(this.out, "_syncreq", SyncDB.Meteor.SyncReq,
		{ id : s, version : new serialization.Array(int), filter : new serialization.Object(Or) });
	regtype(this.out, "_select", SyncDB.Meteor.Select,
		{ id : s, filter : SyncDB.Serialization.Filter });
	regtype(this.out, "_insert", SyncDB.Meteor.Insert,
		{ id : s, row : this.parser_out });
	// TODO: is an update allowed to change hidden fields?
	// insert is, so in principle this should be allowed
	regtype(this.out, "_update", SyncDB.Meteor.Update,
		{ id : s, row : this.parser_in });

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
		    if (this.sync_callback)
			this.sync_callback(o.version, o.rows);
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
    get_empty : function(cb) {
	var n = {};
	for (var field in this.schema.m) {
	    if (!cb || cb(this.schema.m[field])) {
		n[field] = false;
	    }
	}
	return n;
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
    low_update : function(key, row, callback, orow) {
	var id = UTIL.get_unique_key(5, this.requests);
	row[this.schema.key] = key;
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Update(id, row));
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
    sync : function(version) {
	UTIL.trace("SyncDB.MeteorTable#sync has been called for unknown reasons.");
	// this.send(new SyncDB.Meteor.SyncReq("foo", version));
    },
    low_insert : function(row, callback) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Insert(id, row));
	return id;
    },
    request_sync : function(version, filters, cb) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.sync_callback = cb;
	this.send(new SyncDB.Meteor.SyncReq(id, version, filters));
    }
});
SyncDB.LocalTable = SyncDB.Table.extend({
    constructor : function(name, schema, db) {
	var done = 3;
	this.config = new SyncDB.TableConfig("_syncdb_"+name);
	this.base(name, schema, db);
	this.config.get(this.ready_ea.get_cb());
	this.ready(this.M(function() {
	    if (this.config.schema().hashCode() != schema.hashCode()) {

		this.prune();
	    }

	    this.config.schema(schema);
	    //this.sync(this.config.version());
	}));
	this.I = {};
	for (var i = 0; i < schema.fields.length; i++) {
	    var type = schema.fields[i];
	    if (type.is_indexed || type.is_key) {
		var field = type.name;
		//UTIL.log("generating index for %s", field);
		this.I[field] = this.index("_syncdb_"+this.name+"_I"+field, type, schema.m[schema.key]);
		SyncDB.LocalField.prototype.get.call(this.I[field], this.ready_ea.get_cb());
		//UTIL.log("INDEX: %o", this.I[field]);
	    }
	}
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
	return SyncDB.LS.is_permanent;
    },
    index : function(name, field_type, key_name) {
	return field_type.get_index(name, field_type, key_name);
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

		SyncDB.LS.remove(k, this.M(function(error, value) { // TODO: make useful with different storage errors etc.
		    if (!error) {
			if (UTIL.stringp(value)) callback(false, row);
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
	var f = this.M(function(value, callback) {
	    var key = this.schema.key;
	    var k = this.schema.id.get_key(this.name, key, value);
	    //UTIL.log("trying to fetch %o from local storage %o.\n", key, [ this.name, name, value] );
	    SyncDB.LS.get(k, this.M(function(error, value) {
		//UTIL.log("LS returned %s -> %s", k, value);
		if (!error) {
		    if (UTIL.stringp(value)) callback(false, this.parser.decode(serialization.parse_atom(value)));
		    else callback(new SyncDB.Error.NotFound());
		} else callback(error);
	    }));
	});
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
	//UTIL.log("ids: %o\n", ids);
	if (ids.length) {
	    if (ids.length == 1) {
		return f(ids[0], function(error, rows) {
		    callback(error, !error ? [ rows ] : rows);
		});
	    }
	    var failed = false;
	    var c = 0;
	    var aggregate = function(error, row) {
		if (failed) return;
		if (error) {
		    failed = true;
		    callback(error, row);
		    return;
		}
		ids[c++] = row;
		if (c == ids.length) callback(false, ids);
	    };
	    // here comes your event aggregator!
	    for (var i = 0; i < ids.length; i++) {
		f(ids[i], aggregate);
	    }
	    return;
	}

	return callback(false, []);
    },
    prune : function() { // delete everything
    },
    low_update : function(id, row, callback, orow) {
	var type = this.config.schema().id;
	var key = type.name;

	this.select_by(id, this.M(function(error, row_) {
	    if (error) {
		callback(error);
		UTIL.error("Some unexpected error occured. Sorry.");
	    }

	    // TODO: check if row.version != orow.version && orow.version == row_.version.
	    // or rather do this more low level
	    SyncDB.LS.set(type.get_key(this.name, key, row[key]), this.parser.encode(row).render(),
			  this.M(function(error) {
			    if (error) {
				callback(new SyncDB.Error.Set(this, row));
			    } else {
				for (var i in this.I) if (this.I.hasOwnProperty(i)) {
				    // TODO: != is not very optimal. we should at least
				    // check.. erm.. use something like type.eq to check that
				    if (row_[i] != row[i]) {
					type.index_remove(this.I[i], row_[i], row_[key]);
					type.index_insert(this.I[i], row[i], row[key]);
				    }
				}
				callback(false, row);
			    }
			  }));
	    
	    //for (var i in this.I) if (!row.hasOwnProperty(i)) error?

	}));
    },
    low_insert : function(row, callback) {
	var key = this.schema.key;

	SyncDB.LS.set(this.schema.id.get_key(this.name, key, row[key]), this.parser.encode(row).render(),
		      this.M(function (error) {
			//UTIL.log("stored in %o.", this.schema.id.get_key(this.name, key, row[key]));
			if (error) return callback(error, row);
			for (var i in this.I) {
			    if (this.schema.m[i].is_unique || this.I[i].has(row[i]))
				this.schema.m[i].index_insert(this.I[i], row[i], row[key]);
			}
			callback(false, row);
		      }));
    }
});
SyncDB.SyncedTableBase = SyncDB.LocalTable.extend({
    constructor : function() {
	this.base.apply(this, Array.prototype.slice.apply(arguments));
	this.sync_ea = new UTIL.EventAggregator();
    },
    synced : function(f) {
	this.sync_ea.ready(f);
    },
    sync : function(version, rows) {
	// TODO: this should be triggered on completion of all updates, otherwise
	// something might fail and we still believe that we are up to date
	this.synced(this.M(function() {
	    this.config.version(version);
	}));

	for (var i = 0; i < rows.length; i++) {
	    var row = rows[i];
	    if (!row[schema.key]) UTIL.error("error in row %o.", row);
	    this.low_select(schema.id.Equal(row[schema.key]), this.M(function(cb, err, oldrow) {
		// check if version is better than before!

		if (err) {
		    this.low_insert(row, cb);
		} else {
		    this.low_update(row[schema.key], row, cb);
		}
	    }, this.sync_ea.get_cb()));
	}
	if (this.sync_callback) {
	    this.sync_callback(version, rows);
	}
	this.sync_ea.start();
    }
});
SyncDB.SyncedTable = SyncDB.SyncedTableBase.extend({
    constructor : function() {
	this.base.apply(this, Array.prototype.slice.apply(arguments));
	if (this.db) {
	    this.ready(this.M(function() {
		this.db.request_sync(this.version(), {}, UTIL.make_method(this, this.sync));
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
		this.db.request_sync(this.version(), m, UTIL.make_method(this, this.sync));
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
	is_readable : 1,
	is_writable : 1,
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
    is_writable : 0
});
SyncDB.Flags.ReadOnly = SyncDB.Flags.Base.extend({
    toString : function() {
	return "ReadOnly";
    },
    is_readable : 0
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
	},
	get_key : function() {
	    return Array.prototype.slice.apply(arguments).join("_");
	},
	get_index : function(name, field_type, key_name) {
	    /*
	    UTIL.log("will trace 'get_index3'");
	    UTIL.error("get_index3(%o, %o)", name, key_name);
	    */
	    if (this.is_unique || this.is_key)
		return new SyncDB.MappingIndex(name, this, key_name);
	    else //if (this.is_indexed)
		return new SyncDB.MultiIndex(name, this, key_name);
	},
	index_lookup : function(index, key) {
	    if (UTIL.objectp(key) && key.index_lookup) {
		return key.index_lookup(index);
	    } else return index.get(key);
	},
	index_insert : function(index, key, id) {
	    return index.set(key, id);
	},
	index_remove : function(index, key, id) {
	    return index.remove(key, id);
	},
	filter_parser : function() {
	    return new serialization.Bloom(this.hash());
	},
	Equal : function(value) {
	    return new SyncDB.Filter.Equal(this.name, this.parser().encode(value));
	},
	Bloom : function(filter) {
	    return new SyncDB.Filter.Bloom(this.name,
					   this.filter_parser().encode(filter));
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
	if (UTIL.functionp(this.hash)) {
	    return new UTIL.Bloom.Filter(this.bloom_size(),
					 this.bloom_probability(),
					 this.hash());
	} else {
	    UTIL.error("RangeFilters not implemented yet.");
	}
    }
});
SyncDB.Types.Integer = SyncDB.Types.Filterable.extend({
    parser : function() {
	return new serialization.Integer();
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
    get_index : function(name, field_type, key_name) {
	if (this.is_unique || this.is_key)
	    return new SyncDB.CritBitIndex(name, this, key_name);
	else //if (this.is_indexed)
	    //return new SyncDB.MultiIndex(name, this, key_name);
	    return this.base.apply(this, Array.prototype.slice.call(arguments));
    },
    Lt : function(val) {
	return new SyncDB.Filter.Lt(this.name, this.parser().encode(val));
    },
    Le : function(val) {
	return new SyncDB.Filter.Le(this.name, this.parser().encode(val));
    },
    Gt : function(val) {
	return new SyncDB.Filter.Gt(this.name, this.parser().encode(val));
    },
    Ge : function(val) {
	return new SyncDB.Filter.Ge(this.name, this.parser().encode(val));
    }
});
SyncDB.Types.String = SyncDB.Types.Filterable.extend({
    parser : function() {
	return new serialization.String();
    },
    random : function() {
	return UTIL.get_random_key(10);
    },
    toString : function() { return "String"; },
    hash : function() {
	return new UTIL.SHA256.Hash();
    }
});
SyncDB.Types.Image = SyncDB.Types.Base.extend({
    parser : function() {
	return new serialization.Image();
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
    toString : function() { return "Vector"; },
    constructor : function(name, types) {
	this.types = types;
	this.base.apply(this, [ name ].concat(Array.prototype.slice.call(arguments, 2)));
    },
    parser : function() {
	var l = new Array(this.types.length+2);
	l[0] = "_vector";
	l[1] = false;
	for (var i = 0; i < l.length-2; i++)
	    l[i+2] = this.types[i].parser();
	return UTIL.create(serialization.Tuple, l);
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
    get_index : function(name, field_type, key_name) {
	if (this.is_unique || this.is_key)
	    UTIL.error("Ranges cannot be unique (yet).");
	else //if (this.is_indexed)
	    return new SyncDB.RangeIndex(name, this, key_name);
    },
    filter_parser : function() {
	return new serialization.RangeSet(this.parser())
    },
    filter : function() {
	return new CritBit.RangeSet()
    },
    //RangeSet : // TODO
    Overlaps : function(range) {
	return new SyncDB.Filter.Overlaps(this.name, this.parser().encode(range));
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
	return CritBit.get_bit(Math.floow(this.getTime()/1000), size);
    };
};
SyncDB.Date.prototype = Date.prototype;
SyncDB.Types.Date = SyncDB.Types.Base.extend({
    toString : function() { return "Date"; },
    parser : function() {
	return new serialization.Date();
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
    get_index : function(name, field_type, key_name) {
	return this.type.get_index(name, field_type, key_name);
    },
    index_insert : function(index, key, id) {
	for (var i = 0; i < key.length; i++)
	    index.set(key[i], id);
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
    _image : SyncDB.Types.Image
});
SyncDB.Serialization.Schema = serialization.Array.extend({
    constructor : function() {
	this.base(SyncDB.Serialization.Type);
	this.type = "_schema";
    },
    encode : function(schema) {
	return this.base(schema.fields);
    },
    decode : function(atom) {
	return UTIL.create(SyncDB.Schema, this.base(atom));
    }
});
SyncDB.DraftTable = SyncDB.LocalTable.extend({
    constructor : function(name, schema) {
	// add some extra fields to the schema
	// we also need to support deletes?
	// insert/update
	this.base(name, schema);
	this.draft_index = new SyncDB.MappingIndex("_syncdb_DI_" + this.name, schema.m[schema.key], schema.m[schema.key]);
    },
    insert : function(row, cb) {
	row[this.schema.key] = 0;
	return this.create_draft(row, cb);
	this.base(row, cb);
    },
    create_draft : function(row, cb) {
	SyncDB.LocalTable.prototype.insert.call(this, row, this.M(function(err, row_) {
	    if (err) return cb(err, row_);
	    this.draft_index.set(row_[this.schema.key],
				 row [this.schema.key]);
	    cb(err, row_);
	}));
    }
});
SyncDB.Connector = SyncDB.LocalField.extend({
    constructor : function(drafts, online, cb) {
	this.drafts = drafts;
	this.online = online;
	this.cb = cb;
	this.base("_syncdb_connector_"+drafts.name+"_"+online.name,
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
		    this.drafts.draft_index.remove(key);
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
