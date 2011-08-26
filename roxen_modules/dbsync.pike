constant MODULE_STEALTH = 0;

constant module_type = MODULE_TAG;
constant module_name = "Webhaven: SyncDB";

inherit Meteor.ChanelConnecteur;

#include <module.h>
inherit "module";

object table; // tæble
object schema; // schemæ

void create() {
    defvar("channel", Variable.String("control", 0, "Channel to use.",
				      "Name of the channel to request from"
				      "the controller for browser interaction."));

    schema = SyncDB.Schema(
	SyncDB.Types.Integer("id", SyncDB.Flags.Key(),
				    SyncDB.Flags.Automatic(),
				    SyncDB.Flags.Join(([ "two" : "id" ]))),
	SyncDB.Types.String("name"),
	SyncDB.Types.String("email"),
	//"date" : SyncDB.Types.Vector(SyncDB.Types.Date(Calendar.Second), ({ "startdate", "stopdate" })),
	/*
	SyncDB.Types.Vector("date", ({ 
	    SyncDB.Types.Date("startdate", Calendar.Second),
	    SyncDB.Types.Date("stopdate", Calendar.Second)
	})),
	*/
	SyncDB.Types.Range("date", 
	    SyncDB.Types.Date("startdate", Calendar.Second),
	    SyncDB.Types.Date("stopdate", Calendar.Second),
	    SyncDB.Flags.Index()
	),
	// two
	SyncDB.Types.String("firstname", SyncDB.Flags.Foreign("two", "firstname")),
	SyncDB.Types.String("lastname", SyncDB.Flags.Foreign("two", "lastname")),
    );
    table = SyncDB.Meteor.Table("one", schema, 
	    SyncDB.MySQL.Table("one", Sql.Sql("mysql://root:Y2sgCUAtjc@localhost/interSync"), schema, "one"));
}

void start(int i, mixed conf) {
    ::start(i, conf);
    register_channel(query("channel"), accept);
}

void accept(object channel, string name) {
    channel->set_cb(table->incoming);
}

string simpletag_schema(string tagname, mapping args, string content,
			RequestID id) {
    return Standards.JSON.encode(schema);
}

string simpletag_channel(string tagname, mapping args, string content,
			RequestID id) {
    return Standards.JSON.encode(query("channel"));
}
