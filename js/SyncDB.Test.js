SyncDB.Test = {
    run : function(db) {
	for (var test in SyncDB.Test) if (SyncDB.Test.hasOwnproperty(test)) {
	    if (test === "run" || test === "Base") continue;
	    try {
		db.clear();
		var o = new SyncDB.Test[test](db);
		var f = UTIL.gauge(function() {
		    o.run();
		});
		UTIL.log("Testsuite '%s' finished in %o ms", test, f);
	    } catch(err) {
		UTIL.log("Testsuite '%s' FAILED", test);
	    }
	}
    }
};

SyncDB.Test.Base = Base.extend({
    constructor : function(db) {
	this.db = db;
    },
    run : function(blob) {
	for (var test in this) {
	    if (UTIL.has_prefix(test, "test_")) {
		try { 
		    var f = UTIL.gauge(UTIL.make_method(this, this[test]));
		    UTIL.log("test '%s' OK: %o ms", test.substr(5), f);
		} catch (err) {
		    UTIL.log("test '%s' failed: %o", test.substr(5), err);
		}
	    }
	}
    },
});
