SyncDB = {
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
SyncDB.LocalField = Base.extend({
    constructor : function(name, parser) {
	this.name = name;
	this.parser = parser;
	//console.log("name: %s, parser: %o\n", name, parser);
    },
    get : function() {
	if (!this.value) {
	    if (localStorage[this.name]) {
		/*try { */
		    this.value = this.parser.decode(serialization.parse_atom(localStorage[this.name]));
		/*} catch(err) {
		    console.log("ERROR: %o\n", err);
		    throw(err);
		}*/
	    } else {
		this.value = undefined;
	    }
	}
	return this.value;
    },
    set : function() {
	if (arguments.length) {
	    this.value = arguments[0];
	}
	if (this.value == undefined) {
	    delete localStorage[this.name];
	} else {
	    console.log("name: %o, parser: %o, this: %o\n", this.name, this.parser, this);
	    localStorage[this.name] = this.parser.encode(this.value).render();
	}
    }
});
SyncDB.MappingIndex = SyncDB.LocalField.extend({
    constructor : function(name, type, id) {
	this.type = type;
	this.id = id;
	this.base(name, new serialization.Object(this.id.parser()));
    },
    set : function(index, id) {
	if (!this.value) 
	    this.value = { };
	this.value[index] = id;
	// adding something can be done cheaply, by appending the tuple
	this.base();
    },
    get : function(index) {
	if (!this.value) this.base(index);
	return this.value[index];
    }
});
SyncDB.MultiIndex = SyncDB.LocalField.extend({
    constructor : function(name, type, id) {
	this.type = type;
	this.id = id;
	this.base(new Serialization.Mapping(this.type, this.is));
    },
    set : function(index, id) {
	if (!this.value) 
	    this.value = { };
	this.value[index] = id;
	// adding something can be done cheaply, by appending the tuple
	this.base();
    },
    get : function(index) {
	if (!this.value) this.base();
	return this.value[index];
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

SyncDB.Schema = Base.extend({
    constructor : function(m) {
	this.m = m;
	this.key;
	for (var name in m) if (m.hasOwnProperty(name)) {
	    if (m[name].is_key) this.key = name;
	    this[name] = m[name];
	}
    },
    hashCode : function() {
	return sha256_digest(this.parser().encode(this).render());
    },
    // maybe in the future, the schema will generate its own parser.
    parser : function() {
	var n = {};
	for (var name in this.m) if (this.m.hasOwnProperty(name)) {
	    n[name] = this.m[name].parser();
	}
	return new serialization.Struct(n);
    }
});
SyncDB.TableConfig = SyncDB.LocalField.extend({
    constructor : function(name) {
	// create self updating field with given serialization for
	// the schema and version, etc.
	this.base(name, new serialization.Struct({
			    version : new serialization.Integer(),
			    schema : new SyncDB.Serialization.Schema(),
			}));
	var v = this.get();
	if (!v) {
	    v = {
		version : 0,
		schema : new SyncDB.Schema({}),
	    };
	    this.set(v);
	}
    },
    version : function() { // table version. to sync missing upstream revisions
	if (arguments.length) {
	    var v = this.get().version = arguments[0];
	    this.set();
	}
	return this.get().version; 
    },
    schema : function() {
	if (arguments.length) {
	    var v = this.get().schema = arguments[0];
	    this.set();
	}
	return this.get().schema; 
    }
});
SyncDB.Table = Base.extend({
    index : function() {
	return null;
    },
    generate_get : function(name, type) {
	var get = this.get(name, type);
	var db = this.db;	
	if (!get) return null
	return function(value, callback) {
	    if (!callback) callback = SyncDB.getcb;
	    get(value, function(error, row) {
		if (!error) return callback(error, row);
		if (!db || error.is_final()) return callback(error, row);
		db["get_by_"+name](value, callback);
		// we wont need to sync the result, otherwise we would already be
		// finished.
		//
		// maybe we want to cache the ones we have requested. but then we
		// have a update callback
	    });
	};
    },
    generate_set : function(name, type) {
	var set = this.set(name, type);
	var db = this.db;
	if (!set) return null
	return function(key, row, callback) {
	    if (!callback) callback = SyncDB.setcb;
	    row[name] = key;
	    if (db) {
		// save to local draft table
		db["set_by_"+name](key, row, function(error, row) {
		    // call function, delete draft

		    if (error) {
			callback(error, row);
			return;
		    }

		    set(key, row, callback);
		});
		return;
	    }

	    set(key, row, callback);
	};
    },
    constructor : function(name, schema, db) {
	this.name = name;
	this.parser = schema.parser();
	this.db = db;
	if (db) db.add_update_callback(UTIL.make_method(this, this.update));
	console.log("schema: %o\n", schema);
	var key;
	for (var field in schema) if (schema.hasOwnProperty(field)) {
	    if (schema[field].is_key) key = schema[field];
	}

	if (!key) throw(SyncDB.Error.Retard("Man, this schema wont work.\n"));

	for (var field in schema) if (schema.hasOwnProperty(field)) {
	    console.log("scanning %s:%o.\n", field, schema[field]);
	    if (schema[field].is_key || schema[field].is_indexed) {
		console.log("   is indexed.\n");

		if (!schema[field].is_key)
		    this["index_"+field] = this.index(field, schema[field], key);
		else console.log("   is key.\n");

		this["get_by_"+field] = this.generate_get(field, schema[field]);
		this["set_by_"+field] = this.generate_set(field, schema[field]);
	    }
	}
    },
    version : function() {
	return this.config.version();
    },
});
SyncDB.LocalTable = SyncDB.Table.extend({
    constructor : function(name, schema, db) { 
	this.config = new SyncDB.TableConfig("_syncdb_"+name);
	if (this.config) {
	    if (this.config.schema().hashCode() != schema.hashCode()) {
		this.prune();
	    } else {
		this.base(name, schema, db);
		return;
	    }
	}

	this.config.schema(schema);
	this.base(name, schema, db);
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
    },
    index : function(name, field_type, key_type) {
	if (key_type.is_unique)
	    return new SyncDB.MappingIndex("_syncdb_I"+name, field_type, key_type);
	
	return null;
    },
    get : function(name, type) {
	var key = this.config.schema().key;
	var f = UTIL.make_method(this, function(value, callback) {
	    var k = type.get_key(this.name, key, value);
	    //console.log("trying to fetch %o from local storage %o.\n", key, [ this.name, name, value] );
	    if (localStorage[k]) callback(0, this.parser.decode(serialization.parse_atom(localStorage[k])));
	    else callback(new SyncDB.Error.NotFound());
	});
	if (type.is_key) {
	    return f;
	} if (type.is_indexed) {
	    var index = this["index_"+name];
	    if (!index) return null;
	    if (type.is_cached)
		return UTIL.make_method(this, function(value, callback) {
		    // probe the index and check sync.
		    var id = index.get(value);
		    if (id) {
			return f(id, callback);
		    } 

		    return callback(new SyncDB.Error.NoSync());
		});
	    else if (type.is_synced) 
		return UTIL.make_method(this, function(value, callback) {
		    // probe the index and check sync.
		    var id = index.get(value);
		    if (id) {
			return f(id, callback);
		    } return callback(new SyncDB.Error.NotFound());
		});
	}

	return null;
    },
    update : function() {
	// this needs to be generated to update index tables and shit like that
    },
    prune : function() { // delete everything
    },
    set : function(name, type) {
	var key = this.config.schema().key;
	var f = UTIL.make_method(this, function(value, row, callback) {
	    console.log("parser: %o\n", this.parser);
	    try {
		localStorage[type.get_key(this.name, key, value)] = this.parser.encode(row).render();
		callback(0, row);
	    } catch(err) {
		console.log("SET FAILED: %o", err);
		callback(new SyncDB.Error.Set(this, row));
	    }
	});
	if (type.is_key) {
	    return f;
	} else if (type.is_indexed) {
	    // check for name in index and store there. could fail if it is not found
	    var index = this["index_"+name];
	    if (!index) {
		console.log("index missing: %o, %o", name, type);
		return null;
	    }
	    return UTIL.make_method(this, function(value, row, callback) {
		index.set(value, row[key]);
		return f(row[key], row, callback);
	    });
	}
    }
});
SyncDB.Flags = {
    Base : Base.extend({ 
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
SyncDB.Flags.Auto = SyncDB.Flags.Base.extend({ });
SyncDB.Flags.AutoInc = SyncDB.Flags.Auto.extend({ });
SyncDB.Types = {
    Base : Base.extend({
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
