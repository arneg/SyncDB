// vim:foldmethod=syntax
UTIL.Base = Base.extend({
    M : function(f) {
	return UTIL.make_method(this, f);
    }
});
SyncDB = {
    throwit : function(err) {
	console.log("error: %o", err);
	console.trace();
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
	    console.log("SUCCESS: saved %o\n", row);
	} else {
	    console.log("FAIL: could not save %o\n", error);
	    console.trace();
	}
    },
    getcb : function(error, row) {
	if (!error) {
	    console.log("FETCHED: %o\n", row);
	} else {
	    console.log("FAIL: could not fetch %o\n", error);
	    console.trace();
	}
    }
};
SyncDB.KeyValueMapping = UTIL.Base.extend({
    constructor : function() {
	this.m = {};
    },
    is_permanent : false,
    set : function(key, value, cb) {
	this.m[key] = value;
	cb(false, value);
    },
    get : function(key, cb) {
	cb(false, this.m[key]);
    },
    remove : function(key, cb) {
	cb(false, delete this.m[key]);
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
	    } catch (err) {
		cb(err);
		return;
	    }
	    cb(false, value);
	},
	is_permanent : true,
	get : function(key, cb) {
	    var value;
	    try {
		value = localStorage[key];
	    } catch(err) {
		cb(err);
		return;
	    }

	    cb(false, value);
	},
	remove : function(key, cb) {
	    var value;
	    try {
		value = delete localStorage[key];
	    } catch (err) {
		cb(err);
		return;
	    }

	    cb(false, value);
	},
	toString : function() {
	    return "SyncDB.KeyValueStorage";
	}
    });
}
if (UTIL.App.is_ipad || UTIL.App.is_phone || UTIL.App.has_local_database) {
    SyncDB.KeyValueDatabase = UTIL.Base.extend({
	constructor : function(cb) {
		console.log("will open db");
		this.db = openDatabase("SyncDB", "1.0", "SyncDB", 5*1024*1024);
		try {
		    console.log("creating transaction.");
		    this.db.transaction(this.M(function (tx) {
			try {
			    console.log("trying create");
			    tx.executeSql("CREATE TABLE IF NOT EXISTS sLsA (key VARCHAR(255) PRIMARY KEY, value BLOB);", [],
					  this.M(function(tx, data) {
					    console.log("no error: %o", tx);
					    this.M(cb)(false);
					  }),
					  this.M(function(tx, err) {
					    console.log("error: %o", err);
					    this.M(cb)(err);
					  }));
			    console.log("seems to have worked!");
			} catch(err) {
			    this.M(cb)(err);
			}
		    }));
		} catch (err) {
		    this.M(cb)(err);
		}
		console.log("db opened.");
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
		    tx.executeSql("DELETE FROM sLsA WHERE key=?;", [key],
				  this.M(function (tx, data) {
				      cb(false);
				      this.replay();
				  }),
				  this.M(function (tx, err) {
				      cb(err);
				      this.replay();
				  })
		    );
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
	toString : function() {
	    return "SyncDB.KeyValueDatabase";
	}
    });
    try {
	SyncDB.LS = new SyncDB.KeyValueDatabase(function (err) {
	    console.log("%o", err);
	    if (err) {
		SyncDB.LS = new (SyncDB.KeyValueStorage || SyncDB.KeyValueMapping)();
	    }
	    console.log("REMOVING q");
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
	SyncDB.LS.get(this.name, this.M(function(err, value) {
		console.log("initialized field %s", this.name);
		if (UTIL.stringp(value))
		    this.value = this.parser.decode(serialization.parse_atom(value));
		else {
		    this.value = def;
		    SyncDB.LS.set(this.name, this.parser.encode(this.value).render(), function() {});
		}
		this.def = undefined;
	}));
	//console.log("name: %s, parser: %o\n", name, parser);
    },
    get : function(cb) {
	if (!this.value) {
	    SyncDB.LS.get(this.name, this.M(function(err, value) {
		if (err) {
		    cb(undefined);
		} else {
		    if (UTIL.stringp(value))
			this.value = this.parser.decode(serialization.parse_atom(value));
		    cb(this.value);
		}
	    }));
	    return this;
	}
	cb(this.value);
	return this;
    },
    set : function() {
	console.log("name: %o, parser: %o, this: %o\n", this.name, this.parser, this);
	if (arguments.length) {
	    if (this.def) {
		this.def = undefined;
	    }
	    this.value = arguments[0];
	}
	if (this.value == undefined) {
	    SyncDB.LS.remove(this.name, function () {});
	} else {
	    SyncDB.LS.set(this.name, this.parser.encode(this.value).render(), function () {});
	}
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
	    SyncDB.throwit("You are too early!!");
	this.value[index] = id;
	// adding something can be done cheaply, by appending the tuple
	this.base();
    },
    get : function(index) {
	if (!this.value)
	if (!this.value) 
	    SyncDB.throwit("You are too early!!");
	if (!this.value[index]) return [];
	return [ this.value[index] ];
    }
});
SyncDB.MultiIndex = SyncDB.LocalField.extend({
    constructor : function(name, type, id) {
	this.type = type;
	this.id = id;
	console.log("%o %o %o", name, type, id);
	this.base(name, new serialization.Object(new serialization.SimpleSet()), {});
    },
    set : function(index, id) {
	if (!this.value) 
	    SyncDB.throwit("You are too early!!");
	if (!this.value[index]) {
	    this.value[index] = { };
	}
	this.value[index][id] = 1;
	// adding something can be done cheaply, by appending the tuple
	console.log(">> %o", this.value);
	this.base();
    },
    get : function(index) {
	if (!this.value)
	    SyncDB.throwit("You are too early!!");
	if (!this.value || !this.value[index]) return [];
	return UTIL.keys(this.value[index]);
    }
});
SyncDB.Serialization = {};
SyncDB.Serialization.Flag = serialization.String.extend({
    constructor : function() {
	this.base();
	this.type == "_flag";
    },
    encode : function(flag) {
	return this.base(flag.toString());
    },
    decode : function(atom) {
	return new SyncDB.Flags[atom.data]();
    }
});
SyncDB.Serialization.FieldType = serialization.Struct.extend({
    constructor : function() {
	this.base({
	    type : new serialization.String(), 
	    flags : new serialization.Array(new SyncDB.Serialization.Flag())
	});
	this.type == "_type";
    },
    encode : function(type) {
	return this.base({ type : type.toString(), flags : type.flags });
    },
    can_encode : function(o) {
	return UTIL.objectp(o) && o instanceof SyncDB.Types.Base;
    },
    decode : function(atom) {
	var a = this.base(atom);
	var t = new SyncDB.Types[a.type]();
	t.constructor.apply(t, a.flags);
	return t;
    }
});
SyncDB.Serialization.Schema = serialization.Object.extend({
    constructor : function() {
	this.base(new SyncDB.Serialization.FieldType());
	this.type = "_schema";
    },
    encode : function(schema) {
	return this.base(schema.m);
    },
    decode : function(atom) {
	return new SyncDB.Schema(this.base(atom));
    }
});
SyncDB.Schema = UTIL.Base.extend({
    constructor : function(m) {
	this.m = m;
	this.key;
	for (var name in m) if (m.hasOwnProperty(name)) {
	    if (m[name].is_key) this.key = name;
	    this[name] = m[name];
	}
    },
    hashCode : function() {
	//return sha256_digest(this.parser().encode(this).render());
	return 1;
    },
    // maybe in the future, the schema will generate its own parser.
    parser : function(filter) {
	var n = {};
	for (var name in this.m) if (this.m.hasOwnProperty(name)) {
	    if (!filter || filter(name, this.m[name]))
		n[name] = new serialization.Or(new serialization.False(),
					       this.m[name].parser());
	}
	return new serialization.Struct(n, "_schema");
    }
});
SyncDB.TableConfig = SyncDB.LocalField.extend({
    constructor : function(name) {
	// create self updating field with given serialization for
	// the schema and version, etc.
	this.base(name, new serialization.Struct({
			    version : new serialization.Integer(),
			    schema : new SyncDB.Serialization.Schema()
			}), 
		  {
		    version : 0,
		    schema : new SyncDB.Schema({})
		  });
    },
    version : function() { // table version. to sync missing upstream revisions
	if (arguments.length) {
	    var v = this.value.version = arguments[0];
	    this.set();
	}
	return this.value.version; 
    },
    schema : function() {
	if (arguments.length) {
	    var v = this.value.schema = arguments[0];
	    this.set();
	}
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
	this.I = {};
	if (db) db.add_update_callback(this.M(this.update));
	console.log("schema: %o\n", schema);
	var key = schema.key;

	if (!key) SyncDB.throwit(SyncDB.Error.Retard("Man, this schema wont work.\n"));

	for (var field in schema) if (schema.hasOwnProperty(field)) {
	    console.log("scanning %s:%o.\n", field, schema[field]);
	    if (schema[field].is_key || schema[field].is_indexed) {
		console.log("   is indexed.\n");

		if (!schema[field].is_key) {
		    this.I[field] = this.index(field, schema[field], key);
		}
		else console.log("   is key.\n");

		this["select_by_"+field] = this.generate_select(field, schema[field]);

		if (schema[field].is_unique)
		    this["update_by_"+field] = this.generate_update(field, schema[field]);
	    }
	}
    },
    get_version : function() {},
    index : function() {
	return null;
    },
    generate_select : function(name, type) {
	var select = this.select(name, type);
	var db = this.db;	
	if (!select) SyncDB.throwit("could not generate select() for %o %o\n", name, type);
	return function(value, callback) {
	    if (!callback) callback = SyncDB.getcb;
	    select(value, function(error, row) {
		if (!error) return callback(error, row);
		if (!db) return callback(error, row);
		db["select_by_"+name](value, callback);
		// we wont need to sync the result, otherwise we would already be
		// finished.
		//
		// maybe we want to cache the ones we have requested. but then we
		// have a update callback
	    });
	};
    },
    generate_update : function(name, type) {
	var update = this.update(name, type);
	var db = this.db;
	if (!update) SyncDB.throwit([ "could not generate update() for %o %o\n", name, type] );
	return function(key, row, callback) {
	    if (!callback) callback = SyncDB.setcb;
	    row[name] = key;
	    if (db) {
		// save to local draft table
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
SyncDB.MeteorTable = SyncDB.Table.extend({
    constructor : function(name, schema, channel, db) {
	this.requests = {};	
	this.channel = channel;
	this.atom_parser = new serialization.AtomParser();
	this.base(name, schema, db);
	var int = new serialization.Integer();
	var s = new serialization.String();
	this.incoming = {
	    _select : new serialization.Struct({
		id : s,
		row : this.parser_in
	    }, "_select"),
	    _update : new serialization.Struct({
		id : s,
		row : this.parser_in
	    }, "_update"),
	    _error : new serialization.Struct({
		id : s,
		error : s
	    }, "_error"),
	    _update : new serialization.Struct({
		id : s,
		row : this.parser_in
	    }, "_update")
	};
	this.out = {
	    _select : new serialization.Struct({
		id : s,
		// it does not make sense here to allow for
		// a SELECT on hidden values. They should only
		// be set by the client.
		row : this.parser_in
	    }, "_select"),
	    _update : new serialization.Struct({
		id : s,
		row : this.parser_in
	    }, "_update"),
	    _insert : new serialization.Struct({
		id : s,
		row : this.parser_out
	    }, "_insert")
	};

	channel.set_cb(this.M(function(data) {
	    var a = this.atom_parser.parse(data);
	    for (var i = 0; i < a.length; i++) {
		var o;
		if (!this.incoming[a[i].type]) {
		    meteor.debug("dont know how to handle %o", a[i]);
		    continue;
		}
		try {
		    o = this.incoming[a[i].type].decode(a[i]);
		} catch (err) {
		    meteor.debug("decoding %o failed: %o\n", a[i], err);
		    continue;
		}

		/*
		if (a[i].type == "_update") {
		    this.call_update_callback(o);
		    continue;
		}
		*/

		var f = this.requests[o.id];

		if (f) {
		    if (a[i].type == "_error") {
			f(o.error);	    
		    } else {
			f(0, o.row);	    
		    }
		} else meteor.debug("could not find reply handler for %o:%o\n", a[i].type, o);
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
    register_request : function(id, callback) {
	this.requests[id] = callback;
    },
    select : function(name, type) {
	return this.M(function(value, callback) {
	    var id = UTIL.get_unique_key(5, this.requests);	
	    var o = this.get_empty(function (type) { return !type.is_hidden; });
	    console.log("name: %o, value: %o\n", name, value);
	    o[name] = value;
	    this.requests[id] = callback;
	    this.channel.send(this.out._select.encode({ row : o, id : id }).render());
	    return id;
	});
    },
    update : function(name, type) {
	return this.M(function(value, row, callback) {
	    var id = UTIL.get_unique_key(5, this.requests);	
	    row[name] = value;
	    this.requests[id] = callback;
	    this.channel.send(this.out._update.encode({ row : row, id : id }).render());
	    console.log("METEOR SET.");
	    return id;
	});
    },
    insert : function(row, callback) {
	var id = UTIL.get_unique_key(5, this.requests);
	this.requests[id] = callback;
	this.channel.send(this.out._insert.encode({ row: row, id: id }).render());
	return id;
    }
});
SyncDB.LocalTable = SyncDB.Table.extend({
    constructor : function(name, schema, db) { 
	this.config = new SyncDB.TableConfig("_syncdb_"+name).get(this.M(function() {
	    if (this.config.schema().hashCode() != schema.hashCode()) {
		this.prune();
	    }

	    this.config.schema(schema);
	    if (db) db.get_version(function(version) {
		if (version > this.version()) {
		    // update all missing fields, depending on sync. maybe all
		    //
		    // server would no which things are either
		    //  - fully synced
		    //  - cached (^^ works the same here)
		    //  - irrelevant
		}
	    });
	}));
	this.base(name, schema, db);
    },
    is_permanent : function() {
	return SyncDB.LS.is_permanent;
    },
    index : function(name, field_type, key_type) {
	if (field_type.is_unique)
	    return new SyncDB.MappingIndex("_syncdb_I"+name, field_type, key_type);
	else if (field_type.is_indexed) 
	    return new SyncDB.MultiIndex("_syncdb_I"+name, field_type, key_type);
	
	return null;
    },
    select : function(name, type) {
	var f = this.M(function(value, callback) {
	    var key = this.schema.key;
	    var k = type.get_key(this.name, key, value);
	    //console.log("trying to fetch %o from local storage %o.\n", key, [ this.name, name, value] );
	    SyncDB.LS.get(k, this.M(function(error, value) {
		if (!error) {
		    if (UTIL.stringp(value)) callback(false, this.parser.decode(serialization.parse_atom(value)));
		    else callback(new SyncDB.Error.NotFound());
		} else callback(error);
	    }));
	});
	if (type.is_key) {
	    return f;
	} else if (type.is_indexed) {
	    var index = this.I[name];
	    if (!index) SyncDB.throwit("Could not find index "+name);
	    if (!type.is_unique)
		return this.M(function(value, callback) {
		    // probe the index and check sync.
		    var ids = index.get(value);
		    console.log("ids: %o\n", ids);
		    if (ids.length) {
			if (ids.length == 1) {
			    return f(ids[0], callback);
			}
			var failed = 0;
			var c = 0;
			var aggregate = function(error, row) {
			    if (error) {
				if (!failed) {
				    failed = 1;
				    callback(error, row);
				}
				return;
			    }
			    ids[c++] = row;
			    if (c == ids.length) callback(0, ids);
			};
			// here comes your event aggregator!
			for (var i = 0; i < ids.length; i++) {
			    f(ids[i], aggregate);
			}
			return;
		    } 

		    return callback(new SyncDB.Error.NoSync());
		});
	    else// if (type.is_synced) 
		return this.M(function(value, callback) {
		    // probe the index and check sync.
		    var ids = index.get(value);
		    if (ids.length) {
			return f(ids[0], callback);
		    } 
		    if (type.is_unique) 
			return callback(new SyncDB.Error.NotFound());
		    else
			return callback(0, []);
		});
	}

	return null;
    },
	     /*
    update : function() {
	// this needs to be generated to update index tables and shit like that
    },
    */
    prune : function() { // delete everything
    },
    update : function(name, type) {
	var f = this.M(function(value, row, callback) {
	    var key = this.config.schema().key;
	    console.log("parser: %o, data: %o\n", this.parser, row);
	    SyncDB.LS.set(type.get_key(this.name, key, value), this.parser.encode(row).render(),
			  this.M(function(error) {
			    if (error) {
				callback(new SyncDB.Error.Set(this, row));
			    } else {
				callback(0, row);
			    }
			  }));
	});
	if (type.is_indexed || type.is_key) {
	    return this.M(function(value, row, callback) {
		var key = this.config.schema().key;
		if (!row[name]) row[name] = value;
		for (var i in this.I) {
		    this.I[i].set(row[i], row[key]);
		    console.log();
		}
		return f(row[key], row, callback);
	    });
	}

	console.log("Could not generate update for %o %o", name, type);
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
		    this.I[i].update(row[i], row[key]);
		    console.log("update %o=%o(%o) in %o(%o)", row[i], row[key], key, this.I[i], i);
		}
		SyncDB.LS.set(this.schema[key].get_key(this.name, key, row[key]), this.parser.encode(row).render(),
			      this.M(function (error) {
				  console.log("stored in %o.", this.schema[key].get_key(this.name, key, row[key]));
				  if (error) callback(error, row);
				  else callback(false, row);
			      })
		);
	    } else callback(error, row);
	});

	if (this.db) {
	    this.db.insert(row, f);
	} else {
	    f(0, row);
	}
    }
});
SyncDB.Flags = {
    Base : UTIL.Base.extend({ 
	toString : function() {
	    return "Base";
	}
    }),
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
    is_key : 1
});
SyncDB.Flags.Index = SyncDB.Flags.Base.extend({ 
    toString : function() {
	return "Index";
    },
    is_indexed : 1
});
SyncDB.Flags.Cached = SyncDB.Flags.Index.extend({
    toString : function() {
	return "Cached";
    },
    is_cached : 1
});
SyncDB.Flags.Sync = SyncDB.Flags.Index.extend({
    toString : function() {
	return "Sync";
    },
    is_synced :1
});
SyncDB.Flags.Hashed = SyncDB.Flags.Base.extend({
    toString : function() {
	return "Hashed";
    },
    transform : function(f) {
	return function(data) {
	    return new SHA256.update(f(data)).digest();
	}
    }
});
SyncDB.Flags.Auto = SyncDB.Flags.Base.extend({ 
    is_automatic: 1,
});
SyncDB.Flags.AutoInc = SyncDB.Flags.Auto.extend({ });
SyncDB.Types = {
    Base : UTIL.Base.extend({
	constructor : function() {
	    this.flags = Array.prototype.slice.apply(arguments);
	    //console.log("creating %s with %d arguments.\n", this.toString(), arguments.length);
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
		//console.log("scanning %o\n", this.flags[i]);
		for (var name in this.flags[i]) {
		    //console.log(name);
		    if (UTIL.has_prefix(name, "is_")) {
			if (!this.hasOwnProperty(name)) this[name] = this.flags[i][name];
		    }
		}
	    }
	},
	get_key : function() {
	    return Array.prototype.slice.apply(arguments).join("_");
	}
    }),
};
SyncDB.Types.Integer = SyncDB.Types.Base.extend({
    parser : function() {
	return new serialization.Integer();
    },
    toString : function() { return "Integer"; }
});
SyncDB.Types.String = SyncDB.Types.Base.extend({
    parser : function() {
	return new serialization.String();
    },
    toString : function() { return "String"; }
});
