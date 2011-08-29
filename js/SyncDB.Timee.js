SyncDB.Timee = { 
    class_counter : 0,
    Sheet : Base.extend({
	constructor : function() {
	    document.body.appendChild(document.createElement("style"));
	    this.rules = {};
	    this.s = document.styleSheets[document.styleSheets.length-1];
	    UTIL.log("%o", this.s);
	    if (!this.s.addRule) {
		this.s.addRule = UTIL.make_method(this.s,
		    function(selector, rules, index) {
			this.insertRule(selector+" {"+rules+"}", index);
		    });
	    }
	    return this.s;
	},
	Rule : function(c) {
	    if (!this.rules[c]) {
		this.s.addRule(c, "");
		this.rules[c] = this.s.cssRules[this.s.cssRules.length-1];
	    }
	    return this.rules[c];
	}
    }),
    S : function() {
	var s = new SyncDB.Timee.Sheet();
	SyncDB.Timee.S = function() { return s; };
	return s;
    },
};
SyncDB.Timee.EventManager = Base.extend({
    constructor : function(table, timee) {
	this.table = table;
	this.timee = timee;
	this.n = ++SyncDB.Timee.class_counter;
	this.c = "_syncdb_rand"+this.n;
	this.rule = SyncDB.Timee.S().Rule("."+this.c);
	this.events = {};
    },
    fade : function() {
	this.rule.style["z-index"] = "-1";
    },
    unfade : function() {
	this.rule.style.removeProperty("z-index");
    },
    hide : function() {
	this.rule.style.display = "none";
    },
    unhide : function() {
	if (this.rule.style.display.search("none") != -1)
	    this.rule.style.removeProperty("display");
    },
    show : function() {
	this.rule.style.display = "block !important";
    },
    unshow : function() {
	if (this.rule.style.display.search("block") != -1)
	    this.rule.style.removeProperty("display");
    },
    toggle : function() {
	if (this.rule.style.display = "none") {
	    this.show();
	} else this.hide();
    },
    add_event : function(id, e) {
	this.events[id] = e;
	e.add_class(this.c);
	return e;
    }
});
SyncDB.Timee.EventSet = SyncDB.Timee.EventManager.extend({
    constructor : function(table, timee, factory) {
	this.base(table, timee);
	this.categories = [];
	this.factory = factory;
	// set up the class definition
    },
    add_category : function(c) {
	this.categories.push(c);
    },
    _change : function(rows) {
    },
    add_event : function(id, e) {
	e = this.base(id, e);
	for (var i = 0; i < this.categories.length; i++) {
	    if (e.matches(this.categories[i].filter)) {
		this.categories[i].add_event(id, e);
	    }
	}
	return e;
    },
    set : function(id, row) {
	if (!this.events[id])
	    this.add_event(id, this.factory(row));
	else this.events[id].update(row);
	return this.events[id];
    }
});
SyncDB.Timee.Event = Base.extend({
    constructor : function(id, o) {
	this.o = o;
    },
    matches : function() {
	return true;
    },
    update : function() {
    }
});
SyncDB.Timee.NodeEvent = SyncDB.Timee.Event.extend({
    add_class : function(c) {
	UTIL.addClass(this.o, c);
    },
    remove_class : function(c) {
	UTIL.removeClass(this.o, c);
    }
});
SyncDB.Timee.Category = SyncDB.Timee.EventManager.extend({
    constructor : function(table, timee, filter) {
	this.base(table, timee);
	this.filter = filter;
    },
    add_event : function(id, e) {
	this.base(id, e);
    },
    hideother : function() {}
});
