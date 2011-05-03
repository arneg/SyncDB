SyncDB.Test = {
    low_run : function(tests, i, log, cb, db) {
	if (i >= tests.length) return cb(false);

	db.config.get(UTIL.make_method(this, function() {
	    var o = new SyncDB.Test[tests[i]](db);

	    console.log("clearing db.\n");
	    db.clear(UTIL.make_method(this, function () {
		try { 
		    UTIL.agauge(o, o.run,
				UTIL.make_method(this, function (f, err) {
			log(f, tests[i], err);
			if (err) cb(err);
			else this.low_run(tests, i+1, log, cb, db);
		    }));
		} catch (err) {
		    log(tests[i], err);
		    cb(err);
		}
	    }));
	}));
    },
    run : function(db, cb) {
	db.config.get(UTIL.make_method(this, function() {
	    var tests = [];	
	    for (var test in SyncDB.Test) if (SyncDB.Test.hasOwnProperty(test)) {
		if (test === "run" || test === "low_run" || test === "Suite") continue;
		tests.push(test);
	    }
	    var log = function (f, test, err) {
		if (!err)
		    UTIL.log("Testsuite '%s' OK: %o s", test, f);
		else
		    UTIL.log("Testsuite '%s' failed: %o", test, err);
	    };

	    if (tests.length) this.low_run(tests, 0, log, cb, db);
	}));
    }
};

SyncDB.Test.Suite = Base.extend({
    constructor : function(db) {
	if (!db) UTIL.trace();
	this.db = db;
	this.schema = db.config.schema();
	this.uniques = {};
	UTIL.log("initialized %o", this);
	for (var name in this.schema.m) {
	    if (this.schema.m[name].is_unique) {
		this.uniques[name] = {};
	    }
	}
	this.rows = [];
    },
    random_row : function() {
	var row = {};
	for (var name in this.schema.m) {
	    if (this.schema.m[name].is_automatic) {
		continue;
	    }
	    do {
		row[name] = this.schema.m[name].random(); 
	    } while (this.schema.m[name].is_unique && this.uniques[name][row[name]]);
	    if (this.schema.m[name].is_unique)
		this.uniques[name][row[name]] = row;
	}
	return row;
    },
    low_run : function(tests, i, log, cb) {
	if (!cb) UTIL.trace();
	if (i >= tests.length) return cb(false);

	UTIL.log("running test %o", tests[i]);

	try { 
	    UTIL.agauge(this, this[tests[i]], 
			UTIL.make_method(this, function (f, err) {
		log(f, tests[i], err);
		if (err) cb(err);
		else this.low_run(tests, i+1, log, cb);
	    }));
	} catch (err) {
	    log(tests[i], err);
	    cb(err);
	}
    },
    run : function(cb) {
	var tests = [];	
	for (var test in this) {
	    if (UTIL.has_prefix(test, "test_")) {
		tests.push(test);
	    }
	}
	tests = tests.sort();
	UTIL.log("%o starting testsuite: %o", this, tests);
	var log = function (f, test, err) {
	    if (!err)
		UTIL.log("test '%s' OK: %o s", test.substr(5), f);
	    else
		UTIL.log("test '%s' failed: %o", test.substr(5), err);
	};

	if (tests.length) this.low_run(tests, 0, log, cb);
    }
});
SyncDB.Test.Simple = SyncDB.Test.Suite.extend({
    constructor : function(db) {
	this.base(db);
	this.rows = [];
    },
    test_0insert : function(cb) {
	if (!cb) {
	    UTIL.log("no cb in test_insert");
	} else {
	    UTIL.log("cb does exist");
	}
	var c = 0;
	var error = false;
	for (var i = 0; i < 2; i++) {
	    c++;
	    var rrow;
	    this.db.insert(rrow = this.random_row(), UTIL.make_method(this, function(err, row) {
		if (err) {
		    cb(err);
		    error = err;
		} else {
		    if (!row.id) {
			UTIL.log("BAD ROW: %o, %o", rrow, row);
		    }
		    this.rows.push(row);
		}
		if (!error && !--c) cb(false);
	    }));
	}
    },
    test_1select : function(cb) {
	// select all indices
	var c = 1;
	cb = UTIL.once(cb, "HEINZ");
	var error = false;
	for (var i = 0; i < this.rows.length; i++) {
	    for (var field in this.schema.m) {
		if (!this.schema.m[field].is_unique) continue;
		//UTIL.log("field: %o %o %o", field, this.schema.m[field], this.uniques);
		c++;
		var index = this.rows[i][field];
		if (!index) UTIL.log("bad row: %o", this.rows[i]);
		//UTIL.log("selecting %o", index);
		this.db["select_by_"+field](index, UTIL.once(function(err, row) {
		    c--;
		    if (error) return;
		    if (err) {
			error = err;
			cb(err);
		    }
		    if (!c) cb(false);
		}));
	    }
	}
	c--;
	if (!c && !error) cb(false);
    },
    test_9remove : function(cb) {
	var c = 1 + this.rows.length;
	cb = UTIL.once(cb);
	var error = false;
	for (var i = 0; i < this.rows.length; i ++) {
	    this.db.remove_by(this.rows[i][this.schema.key], function(err, row) {
		c--;
		if (error) return;
		if (err) {
		    error = err;
		    console.log("rmove of %o failed with %o, %o", i, err, row);
		    cb(err);
		}
		if (!c) cb(false);
	    });
	}
	c--;
	if (!c && !error) cb(false);
    }
});
SyncDB.Test.Connector = SyncDB.Test.Simple.extend({
    constructor : function(drafts, online) {
	this.connector = new SyncDB.Connector(drafts, online,
					      UTIL.make_method(this, this.commitcb));
	this.base(drafts);
    },
    commitcb : function(key, err, row) {
	this.count--;
	if (this.error) return;
	if (err) {
	    this.error = err;
	    this.cb(err);
	    return;
	}
	if (!this.count) this.cb(false);
    },
    test_2commit : function(cb) {
	this.cb = cb;
	this.count = this.rows.length;
	for (var i = 0; i < this.rows.length; i++) {
	    this.connector.commit(this.rows[i][this.schema.key]);
	}
	this.rows = [];
    }
});
