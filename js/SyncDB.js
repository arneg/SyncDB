// vim:foldmethod=syntax
UTIL.Base = Base.extend({
    M : function(f) {
	return UTIL.make_method(this, f);
    }
});
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
SyncDB.Filter = {};
SyncDB.Filter.Base = UTIL.Base.extend({
    _types : {
	field : new serialization.String()
    },
    constructor : function(field) {
	this.field = field;
	this.args = Array.prototype.slice.apply(arguments);
    }
});
SyncDB.Filter.Or = SyncDB.Filter.Base.extend({
    _types : {
	args : function(p) {
	    return new serialization.Array(p);
	}
    },
    index_lookup : function(table) {
	var results = this.args[0].index_lookup(table)

	for (var i = 1; i < this.args.length; i++) {
	    results = UTIL.array_or(this.args[i].index_lookup(table), results);
	}

	return results;
    }
});
SyncDB.Filter.And = SyncDB.Filter.Or.extend({
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
	var type = table.m[this.field];
	var index = table.get_index(this.field);
	if (!index || !index.overlaps) return 0;

	return index.overlaps(type.parser().decode(this.value));
    }
});
SyncDB.Serialization = {};
SyncDB.Serialization.Filter = serialization.generate_structs({
    _or : SyncDB.Filter.Or,
    _and : SyncDB.Filter.And,
    _equal : SyncDB.Filter.Equal,
    _true : SyncDB.Filter.True,
    _false : SyncDB.Filter.False,
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
    get : function(key, cb) {
	UTIL.call_later(cb, null, false, this.m[key]);
    },
    remove : function(key, cb) {
	var v = this.m[key];
	delete this.m[key];
	UTIL.call_later(cb, null, false, v);
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
	set : function(key, value, cb) {
	    try {
		localStorage[key] = value;
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		UTIL.call_later(cb, null, err);
	    }
	},
	is_permanent : true,
	get : function(key, cb) {
	    var value;
	    try {
		value = localStorage[key];
		UTIL.call_later(cb, null, false, value);
	    } catch(err) {
		UTIL.call_later(cb, null, err);
	    }

	},
	remove : function(key, cb) {
	    try {
		var value = localStorage[key];
		delete localStorage[key];
		UTIL.call_later(cb, null, false, value);
	    } catch (err) {
		UTIL.call_later(cb, null, err);
	    }
	},
	clear : function(cb) {
	    // TODO: remove only sync_db entries. ;_)
	    localStorage.clear();
	    UTIL.call_later(cb, null, false);
	},
	toString : function() {
	    return "SyncDB.KeyValueStorage";
	}
    });
}
if (UTIL.App.is_ipad || UTIL.App.is_phone || UTIL.App.has_local_database) {
    SyncDB.KeyValueDatabase = UTIL.Base.extend({
	constructor : function(cb) {
		UTIL.log("will open db");
		this.db = openDatabase("SyncDB", "1.0", "SyncDB", 5*1024*1024);
		this.init(cb);
	},
	init : function(cb) {
		try {
		    UTIL.log("creating transaction.");
		    this.db.transaction(this.M(function (tx) {
			try {
			    UTIL.log("trying create");
			    tx.executeSql("CREATE TABLE IF NOT EXISTS sLsA (key VARCHAR(255) PRIMARY KEY, value BLOB);", [],
					  this.M(function(tx, data) {
					    UTIL.log("no error: %o", tx);
					    this.M(cb)(false);
					  }),
					  this.M(function(tx, err) {
					    UTIL.log("error: %o", err);
					    this.M(cb)(err);
					  }));
			    UTIL.log("seems to have worked!");
			} catch(err) {
			    this.M(cb)(err);
			}
		    }));
		} catch (err) {
		    this.M(cb)(err);
		}
		UTIL.log("db opened.");
	},
	is_permanent : true,
	q : [],
	get : function(val, cb) {
	    if (this.q) {
		this.q.push(function() { this.get(val, cb); });
	    } else {
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    tx.executeSql("SELECT * FROM sLsA WHERE key=?;", [val], this.M(function(tx, data) {
			if (!data.rows.length) {
			    cb(false, undefined);
			} else if (data.rows.length == 1) {
			    cb(false, data.rows.item(0).value);
			} else {
			    // FUCKUP
			}
			this.replay();
		    }), this.M(function(tx, err) {
			UTIL.log("err: %o", err);
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
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    tx.executeSql("INSERT OR REPLACE INTO sLsA (key, value) VALUES(?, ?);", [ key, val ],
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
		this.q = [];
		this.db.transaction(this.M(function (tx) {
		    tx.executeSql("SELECT * FROM sLsA WHERE key=?;", [key],
			this.M(function(tx, data) {
			    tx.executeSql("DELETE FROM sLsA WHERE key=?;", [key],
					  this.M(function (tx) {
					      UTIL.log("tx: %o. data: %o", tx, data);
					      cb(false, data.rows.item(0).value);
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
	replay : function() {
	    var q = this.q;
	    this.q = undefined;
	    for (var i = 0; i < q.length; i++) {
		q[i].apply(this);
	    }
	},
	clear : function(cb) {
	    if (!cb) cb = function() {};
	    this.db.transaction(this.M(function (tx) {
		tx.executeSql("DROP TABLE sLsA;", [], this.M(function() {
			      this.init(cb);
			  }), function(err) {
			      UTIL.log("db clear errored: %o", Array.prototype.slice.call(arguments));
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
	    UTIL.log("REMOVING q");
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

	if (!this.value) { // cache this, we will fetch
	    SyncDB.LS.get(this.name, this.M(function(err, value) {
		if (err) {
		    UTIL.log("error: %o");
		    cb(undefined);
		} else {
		    if (UTIL.stringp(value))
			this.value = this.parser.decode(serialization.parse_atom(value));
		    else if (this.def) {
			this.value = this.def;
			delete this.def;
			this.sync();
		    }
		    cb(this.value);
		}
	    }));
	    return this;
	}
	UTIL.call_later(cb, null, this.value);
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
/**
 * This is a fake index, which is only used to allow us to use one standard api
 * for the keys aswell.
 */
SyncDB.KeyIndex = Base.extend({
    constructor : function(name, type, id) { },
    get : function(index) {
	return [ index ];
    }
});
SyncDB.MappingIndex = SyncDB.LocalField.extend({
    constructor : function(name, type, id) {
	this.type = type;
	this.id = id;
	this.base(name, new serialization.Object(type.parser()), {});
    },
    set : function(index, id) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	this.value[index] = id;
	// adding something can be done cheaply, by appending the tuple
	this.sync();
    },
    get : function(index) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (!this.value[index]) return [];
	return [ this.value[index] ];
    },
    remove : function(index, value) {
	delete this.value[index];
	this.sync();
    },
    toString : function() {
	return "MappingIndex("+this.name+","+this.type+")";
    },
    values : function() {
	return UTIL.values(this.value);
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
	if (!this.value[index]) {
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
	if (!this.value || !this.value[index]) return [];
	return UTIL.keys(this.value[index]);
    },
    remove : function(index, value) {
	if (!this.value)
	    SyncDB.error("You are too early!!");
	if (this.value[index]) {
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
		    version : ({ }),
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
	if (db) db.add_update_callback(this.M(this.update));
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
		if (type.is_unique)
		    this["update_by_"+field] = this.generate_update(field, type);
		if (type.is_key) {
		    //UTIL.log("   is key.\n");
		    this.update_by = this["update_by_"+field];
		    this.select_by = this["select_by_"+field];
		    this.remove_by
		      = this["remove_by_"+field] =
			this.generate_remove(field, type);
		}

	    }
	}
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
    select : function(filter, callback) {
	var extra = Array.prototype.slice.call(arguments, 2);
	// allow us to generate partial results at least.
	if (!callback) callback = SyncDB.getcb;
	callback = UTIL.once(callback);

	var f = extra.length ? function(error, rows) {
	    callback.apply(window, [error, row].concat(extra));
	} : callback;

	this.low_select(filter, this.M(function(error, rows) {
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
    generate_update : function(name, type) {
	var update = this.update(name, type);
	this["local_update_by_"+name] = update;
	var db = this.db;
	if (!update) SyncDB.error([ "could not generate update() for %o %o\n", name, type] );
	return function(key, row, callback) {
	    if (!callback) callback = SyncDB.setcb;
	    row[name] = key;
	    if (db) {
		// save to local draft table and, erm also:
		// somehow remember the ID so we can act on it later on
		db["update_by_"+name](key, row, function(error, row) {
		    // call function, delete draft

		    if (error) {
			callback(error, row);
			return;
		    }

		    update(key, row, callback);
		});
		return;
	    }

	    update(key, row, callback);
	};
    },
    version : function() {
	return this.config.version();
    },
    add_update_callback : function(cb) {
	// this gets triggered on update / delete
    }
});
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
SyncDB.Meteor.SyncReq = SyncDB.Meteor.Base.extend({});

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
	regtype(this.out, "_syncreq", SyncDB.Meteor.SyncReq,
		{ id : s, version : new serialization.Array(int), filter : SyncDB.Serialization.Filter });
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
			for(var j = 0; j < o.rows.length; j++) {
			    this.sync_callback(o.version, o.rows[j]);
			}
		    continue;
		}

		var f = this.requests[o.id];

		if (f) {
		    if (a[i].type == "_error") {
			f(o.error);
		    } else {
			f(0, o.rows);
		    }
		} else UTIL.log("could not find reply handler for %o:%o\n", a[i].type, o);
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
    update : function(name, type) {
	return this.M(function(value, row, callback) {
	    var id = UTIL.get_unique_key(5, this.requests);
	    row[name] = value;
	    this.requests[id] = callback;
	    this.send(new SyncDB.Meteor.Update(id, row));
	    UTIL.log("METEOR SET.");
	    return id;
	});
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
	this.send(new SyncDB.Meteor.SyncReq("foo", version));
    },
    // TODO: this does not support chaining. we have to do this properly
    insert : function(row, callback) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.requests[id] = callback;
	this.send(new SyncDB.Meteor.Insert(id, row));
	return id;
    }
});
SyncDB.LocalTable = SyncDB.Table.extend({
    constructor : function(name, schema, db) {
	(this.config = new SyncDB.TableConfig("_syncdb_"+name)).get(this.M(function() {
	    if (this.config.schema().hashCode() != schema.hashCode()) {
		UTIL.log("SCHEMA changed. cleaning local databse %o != %o (new)", this.config.schema(), schema);
		UTIL.log("%o vs %o", this.config.schema().hashCode(), schema.hashCode());

		this.prune();
	    } else UTIL.log("SCHEMA unchanged.");

	    this.config.schema(schema);
	    this.sync(this.config.version());
	}));
	this.base(name, schema, db);
	this.I = {};
	for (var i = 0; i < schema.fields.length; i++) {
	    var type = schema.fields[i];
	    if (type.is_index || type.is_key) {
		var field = type.name;
		//UTIL.log("generating index for %s", field);
		this.I[field] = this.index("_syncdb_"+this.name+"_I"+field, type, schema.key);
		//UTIL.log("INDEX: %o", this.I[field]);
	    }
	}
	if (db) {
	    db.sync_callback = UTIL.make_method(this, function(version, row) {
		var db;

		this.config.version(version);

		this.low_select(schema.id.Equal(row[schema.key]), this.M(function(err, oldrow) {
		    // check if version is better than before!

		    if (err) {
			this.local_insert(row, function(err, row) {});
		    } else {
			this["local_update_by_"+schema.key](row[schema.key], row, function(err, oldrow) {});
		    }
		}));

		if (this.sync_callback) {
		    this.sync_callback(version, row);
		}
	    });
	}
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
    index : function(name, field_type, key_type) {
	return field_type.get_index(name, field_type, key_type);
    },
    remove : function(name, type) {
	var f = this.M(function(value, callback) {
	    var key = this.schema.key;
	    var k = type.get_key(this.name, key, value);

	    UTIL.log("select_by(%o)", value);
	    this.select_by(value, this.M(function(err, row) {
		UTIL.log("select_by(%o) : %o %o", value, err, row);
		if (err) return callback(err, row);
		for (var i in this.I) {
		    type.index_remove(this.I[i], row[i], row[key]);
		}

		SyncDB.LS.remove(k, this.M(function(error, value) { // TODO: make useful with different storage errors etc.
		    UTIL.log("LS remove : %o %o", error, value);
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
	    UTIL.call_later(callback, null, error);
	}
	//UTIL.log("ids: %o\n", ids);
	if (ids.length) {
	    if (ids.length == 1) {
		return f(ids[0], callback);
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
	     /*
    update : function() {
	// this needs to be generated to update index tables and shit like that
    },
    */
    prune : function() { // delete everything
    },
    // TODO: first, insert data, then put into INDEX!!!!
    update : function(name, type) {
	var f = this.M(function(value, row, callback) {
	    var key = this.config.schema().key;
	    //UTIL.log("parser: %o, data: %o\n", this.parser, row);
	    SyncDB.LS.set(type.get_key(this.name, key, value), this.parser.encode(row).render(),
			  this.M(function(error) {
			    if (error) {
				callback(new SyncDB.Error.Set(this, row));
			    } else {
				callback(false, row);
			    }
			  }));
	});
	if (type.is_indexed || type.is_key) {
	    return this.M(function(value, row, callback) {
		var key = this.config.schema().key;
		if (!row[name]) row[name] = value;
		this["select_by_" + name](value, this.M(function(err, row_) {
		    if (err) {
			UTIL.error("Some unexpected error occured. Sorry.");
		    }
		    for (var i in this.I) {
			type.index_remove(this.I[i], row_[i], row_[key]);
			type.index_insert(this.I[i], row[i], row[key]);
		    }
		    return f(row[key], row, callback);
		}));
	    });
	}

	SyncDB.error("Could not generate update for %o %o", name, type);
    },
	     /*
    update : function() {
    },
    */
    insert : function(row, callback) { // TODO:: this should rather make the db a draft and store locally, i guess,
				    // then send the _insert (if this.db exists) and if that returns ok, remove draft status
				    // (given no other pending modifications), but i'm lost in the select/update/draft logic.
	var key = this.schema.key;
	var f = this.M(function(error, row) {
	    if (!error) {
		for (var i in this.I) {
		    this.schema.m[i].index_insert(this.I[i], row[i], row[key]);
		    //UTIL.log("update %o=%o(%o) in %o(%o)", row[i], row[key], key, this.I[i], i);
		}
		SyncDB.LS.set(this.schema.id.get_key(this.name, key, row[key]), this.parser.encode(row).render(),
			      this.M(function (error) {
				  //UTIL.log("stored in %o.", this.schema.id.get_key(this.name, key, row[key]));
				  if (error) callback(error, row);
				  else callback(false, row);
			      })
		);
	    } else callback(error, row);
	});

	if (this.db) {
	    this.db.insert(row, f);
	} else {
	    row = UTIL.copy(row);
	    this.schema.get_auto_set(this, function(as) {
		for (var x in as) row[x] = as[x];
		f(false, row);
	    });
	}
    },
    local_insert : function(row, callback) {
	var db = this.db;
	this.db = undefined;
	this.insert(row, callback);
	this.db = db;
    }
});
SyncDB.CachedTable = SyncDB.LocalTable.extend({
    low_select : function(filter, callback) {
	this.base(filter, this.M(function(error, rows) {
	    if (error instanceof SyncDB.Error.NotFound)
		error = new SyncDB.Error.NoSync();
	    callback(error, rows);
	}));
    },
});
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
SyncDB.Types = {
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
		for (var name in this.flags[i]) {
		    //UTIL.log(name);
		    if (UTIL.has_prefix(name, "is_") || UTIL.has_prefix(name, "get_")) {
			if (!this.hasOwnProperty(name)) this[name] = this.flags[i][name];
		    }
		}
	    }
	},
	get_key : function() {
	    return Array.prototype.slice.apply(arguments).join("_");
	},
	get_index : function(name, key_type) {
	    if (this.is_key) 
		return new SyncDB.KeyIndex(name, this, key_type);
	    else if (this.is_unique)
		return new SyncDB.MappingIndex(name, this, key_type);
	    else //if (this.is_indexed)
		return new SyncDB.MultiIndex(name, this, key_type);
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
	Equal : function(value) {
	    return new SyncDB.Filter.Equal(this.name, this.parser().encode(value));
	}
    })
};
SyncDB.Types.Integer = SyncDB.Types.Base.extend({
    parser : function() {
	return new serialization.Integer();
    },
    random : function() {
	return Math.floor(0xffffffff*Math.random());
    },
    toString : function() { return "Integer"; },
    increment : function(old) {
	return old + 1;
    }
});
SyncDB.Types.String = SyncDB.Types.Base.extend({
    parser : function() {
	return new serialization.String();
    },
    random : function() {
	return UTIL.get_random_key(10);
    },
    toString : function() { return "String"; }
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
SyncDB.Range = Base.extend({
    constructor : function(start, stop) {
	this.start = start;
	this.stop = stop;
    }
});
SyncDB.Types.Range = SyncDB.Types.Vector.extend({
    toString : function() { return "Range"; },
    constructor : function(name, from, to) {
	this.base.apply(this, [ name, [ from, to ] ].concat(Array.prototype.slice.call(arguments, 3)));
    },
    parser : function() {
	UTIL.log("%o, %o\n", this.types[0].parser(), this.types[0]);
	return new serialization.Tuple("_range", SyncDB.Range,
				       this.types[0].parser(),
				       this.types[1].parser()).extend({
	    encode : function(range) {
		return this.base([ range.start, range.stop ]);
	    }
	});
    }
});
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
    get_index : function(name, key_type) {
	return this.type.get_index(name, key_type);
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
	UTIL.log("DRAFT INDEX: %o", this.draft_index);
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
	UTIL.log("Connector(%o, %o)", drafts, online);
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
		    //console.log("removing key %o", key);
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
		else this.online.update_by(oid, row, callback);
	    } else this.online.insert(row, callback);
	}));
    }
});
