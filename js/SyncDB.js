SyncDB = {
    Table : Base.extend({
	constructor : function(schema) {
	    foreach (var i in schema) if (schema.hasOwnProperty(i)) {
		if (schema[i].indexable()) {
		    this["get_by_"+i] = function(value, callback) {
			db["get_by_"+i](value, callback);
		    };
		}
	    }
	},
	connect : function(db) {
	    this.db = db;
	},
    }),
    LocaleStorage : Base.extend({
	low_set : function(key, row, callback) {
	    try {
		LocaleStorage[this.schema[i].get_key(this.namespace, key)] = atom_parser.encode(row);
		callback(0, row);
	    } catch(err) {
		callback(1, new SyncDB.Error.Set(this, row));
	    }
	    return;
	},
	constructor : function(namespace, schema, db) {
	    this.namespace = namespace;
	    this.schema = schema;
	    foreach (var i in schema) if (schema.hasOwnProperty(i)) {
		if (schema[i].indexable()) {
		    this["get_by_"+i] = function(value, callback) {
			var o = LocaleStorage[schema[i].get_key(namespace, value)];
			if (o) {
			    callback(0, atom_parser.decode(o));
			    return;
			}
			if (schema[i].is_synced()) {
			    callback(1, new SyncDB.Error.NotFound(this, i));
			} else if (db) {
			    db["get_by_"+i](value, callback);
			}
		    };
		    this["set_by_"+i] = function(key, row, callback) {
			if (!db) return this.low_set(key, row, callback);
			// save to local draft table
			db["set_by_id"+i](key, row, function(error, row) {
			    // call function, delete draft
			    
			    if (error) {
				callback(error, row);
				return;
			    }

			    this.low_set(key, row, callback);
			});
			var o = LocaleStorage[schema[i].get_key(namespace, row)];
			if (o) {
			    callback(0, atom_parser.decode(o));
			    return;
			}
			if (schema[i].is_synced()) {
			    callback(1, new SyncDB.Error.NotFound(this, i));
			} else if (db) {
			    db["get_by_"+i](value, callback);
			}
		    };
		}
	    }
	}
    }),
    Types : {
	Base : Base.extend({})
    }
};
SyncDB.Types.String : SyncDB.Types.Base.extend({
});

var o = new SyncDB.Table({
	     	id : Integer(Index(), FullTextSearch()),
	     	name : Foo()
		foo : Integer(Index())
	     });

o.search({ id : .., name : "John" }, function(error, rows) {
});
o.search_id(23, function (error, row) {
o.get_by_id(23, function (error, row) {
});
o.get_by_foo(23, function (error, row) {
});
