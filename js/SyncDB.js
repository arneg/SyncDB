SyncDB = {
    Error : {
	NoSync : Base.extend({ })
	Set : Base.extend({ })
	NoIndex : Base.extend({ })
    }
};
SyncDB.LocalField = Base.extend({
    constructor : function(name, parser) {
	this.name = name;
	this.parser = parser;
    },
    get : function() {
	if (!this.value) {
	    if (localStorage[this.name]) {
		this.value = parser.decode(localStorage[this.name]);
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
	    localStorage[this.name] = parser.encode(this.value);
	}
    }
});
SyncDB.LocalConfig = new SyncDB.LocalField("_syncdb",
					   new Serialization.Types.Struct({
						version : new Serialization.Types.Integer(),
						schema : new SyncDB.Serialization.Schema(),
					   }));
SyncDB.Schema = Base.extend({
    hashCode : function() {
	return 1;
    },
});
SyncDB.TableConfig = SyncDB.LocalField.extend({
    constructor : function(name) {
	// create self updating field with given serialization for
	// the schema and version, etc.
	this.base(name, new Serialization());
	var v = this.get();
	if (!v) {
	    v = {
		version : 0,
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
    constructor : function(name, schema, db) {
	this.name = name;
	foreach (var name in schema) if (schema.hasOwnProperty(name)) {
	    if (schema[name].indexable()) {
		this["get_by_"+name] = function(value, callback) {
		    this.get(name, value, function(error, row) {
			if (!error) return callback(error, row);
			if (!db || error.is_final()) return callback(error, row);
			db.get(name, value, callback);
			// we wont need to sync the result, otherwise we would already be
			// finished.
			//
			// maybe we want to cache the ones we have requested. but then we
			// have a update callback
		    });
		};
		this["set_by_"+i] = function(key, row, callback) {
		    if (db) {
			// save to local draft table
			db.set(i, key, row, function(error, row) {
			    // call function, delete draft

			    if (error) {
				callback(error, row);
				return;
			    }

			    this.set(i, key, row, callback);
			});
			return;
		    }

		    this.set(i, key, row, callback);
		};
	    }
	}
	this.config = SyncDB.LocalConfig.get(name);
	if (db) db.add_update_callback(UTIL.make_method(this, this.update));
	if (this.config) {
	    if (this.config.schema().hashCode() != schema.hashCode()) {
		this.prune();
	    } else return;
	}

	this.config.schema(schema);
	this.db = db;
	db.get_version(function(version) {
	    if (version > this.version()) {
		// update all missing fields, depending on sync. maybe all
		//
	    }
	});
    },
    version : function() {
	return this.version;
    },
});
SyncDB.LocalTable = Base.extend({
    get : function(index, value, callback) {
	// probe the index and check sync.
	var v;
	var index = this.config.index(index);
	if (!index) return callback(new SyncDB.Error.NoIndex(index), value);
	v = index.get(value);
	if (v || index.is_sync()) {
	    return callback(0, v);
	} 

	return callback(new SyncDB.Error.NoSync());
    },
    update : function() {
	// this needs to be generated to update index tables and shit like that
    },
    prune : function() { // delete everything
    },
    set : function(key, row, callback) {
	try {
	    localStorage[this.schema[i].get_key(this.namespace, key)] = atom_parser.encode(row);
	    callback(0, row);
	} catch(err) {
	    callback(new SyncDB.Error.Set(this, row));
	}
	return;
    },
    constructor : function(namespace, schema, db) {
	this.namespace = namespace;
	this.schema = schema;
    }
});
SyncDB.Flags = {
    Base : Base.extend({ }),
};
SyncDB.Flags.Unique = SyncDB.Flags.Base.extend({ 
    is_unique : function() {
	return 1;
    }
});
SyncDB.Flags.Index = SyncDB.Flags.Base.extend({ 
    is_indexed : function() {
	return 1;
    }
});
SyncDB.Flags.Auto = SyncDB.Flags.Base.extend({ });
SyncDB.Flags.Sync = SyncDB.Flags.Base.extend({
    is_synced : function() { 
	return 1; 
    }
});
function sha256(s) {
    return "SHA256: " + s;
}
SyncDB.Flags.Hashed = SyncDB.Flags.Base.extend({
    transform : function(f) {
	return function(data) {
	    return sha256(f(data));
	}
    }
});
SyncDB.Flags.AutoInc = SyncDB.Flags.Auto.extend({ });
SyncDB.Types = {
    Base : Base.extend({
	constructor : function() {
	    this.flags = Array.prototype.concat.call(arguments);
	    // BACKWARDS loop for things that are combined recursively
	    for (var i = this.flags.length-1; i; i--) {
		for (var name in this.flags[i]) if (this.flags[i].hasOwnProperty(name)) {
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
		for (var name in this.flags[i]) if (this.flags[i].hasOwnProperty(name)) {
		    if (UTIL.has_prefix(name, "is_")) {
			if (!this.hasOwnProperty(name)) this[name] = this.flags[i][name];
		    }
		}
	    }
	},
	get_key : function(namespace, o) {
	    return namespace + "_" + o.toString();
	}
    }),
};
SyncDB.Types.Integer = SyncDB.Types.Base.extend({
    function : get_parser() {
	return new Serialization.Types.Integer();
    },
});
SyncDB.Types.String : SyncDB.Types.Base.extend({
    function : get_parser() {
	return new Serialization.Types.String();
    },
});
