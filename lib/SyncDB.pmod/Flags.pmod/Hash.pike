inherit .Base;

// A field with this flag is hashed before exposing it to the client. It
// locally only supports equality test.
Crypto.HashState hash;

void create(Crypto.HashState hash) {
    this_program::hash = hash;
}

mixed filter(mixed value) {
    return hash->update((string)value)->digest();
}

buf += sprintf(y->_filter(this, x->_filter(this), "value"));

string _filter(object type, object context) {
    string s = context->symbol("hash", hash);

    if (object_program(type) == SyncDB.Types.Integer) {
	// add integer sha256 hash here,or whatever
	return sprintf("%s->update((string)%%s)->digest()", s);
    } else if (object_program(type) == SyncDB.Types.String) {
	// add integer sha256 hash here,or whatever
	return sprintf("%s->update(%%s)->digest()", s);
    }
}

void encode_json() {
    return "SyncDB.Flags.Hash()";
}

// JS FOLLOWS:
// ===========================================================
// this happens automatically
/*
o = SyncDB.Table("namespace",
    {
	name : SyncDB.Types.String(SyncDB.Flags.Hidden(), SyncDB.Flags.NotNull()),
	id : SyncDB.Types.Integer(SyncDB.Flags.Index(), SyncDB.Flags.AutoIncrement()),
	email : SyncDB.Types.String(SyncDB.Flags.Hash(), SyncDB.Flags.Unique(), SyncDB.Flags.NotNull()),
	date : SyncDB.Types.DateRange(SyncDB.Flags.Sync())
    },
    SyncDB.Flags.SyncAll(),
);
var parser = Serialization.Mapping({
	name : Serialization.Types.String(),
	id : Serialization.Types.Integer(),
	email : Serialization.Types.String(),
	date : Serialization.Types.DateRange()
});
var form = FormCreator.Form({
	name : FormCreator.Types.String(),
	id : FormCreator.Types.Integer(),
	email : FormCreator.Types.String(),
	date : FormCreator.Types.DateRange()
});
*/
o = SyncDB.Table("calendar", (((call tablegenerator("calendar")))));
var calendar_form = FormCreator.Form((((call formgenerator("calendar")))));
// form is json definition, stuff this into your jquery plugin.
var atom = parser.encode(row);

meteor.write(atom);

o.connect(SyncDB.LocalStorage("namespace", SyncDB.Meteor(url)));



// manually written:
o.set_callback(function(error, row) {
});


// fetch if necessary, keep in sync
o.get_by_id(id, function(error, row) { 
    if (error) {
	// do some error processing, collision on..
	if (error is Collision(..)) {
	}
    }
    if (row.id()) {
	// exists already
	return;
    }
    // This row would be a copy and check against revision on set. Like this we can send real update
    // calls. it would also detect failure, or only update when revision matches. This is basically
    // a async CAS.
    form.insert(row, { onSubmit : function() {
	row.set( { 
		    name : "Tiffany"
		 }, 
	    function(error, nrow) {
		if (error) {
		}
	    });
    });
});

function try_again() {
    var transaction = o.start_transaction(); // would start a transaction for the users connection
    var row1 = transaction.get..
    var row2 = transaction.get..

    row1->name(..);
    row2->name(..);
    transaction.add_row(...);
    transaction.commit(function (error, data) {
	if (error) try_again();
    });
}
o.add_row(0, { ... }, function(error, row) {});
o.set_by_name("John Smith", {}, function(error, row) {}); // calls the error callback. "not indexable"

o.get_by_date(Date.now(), Date.now() - 5, function (error, row) {
    o.set_by_id(id, { description: "foo" }, function (error, row) {
	// display error
    });
});

o.set_update_callback(function (row) {
    calender_ui->add_event(row);
});

add_event(row) {
    row.add_update_callback(UTILS.make_method(this, function (row) {
	this.display_row(row);
    }));
}


