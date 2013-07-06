constant MODULE_STEALTH = 0;

//constant module_type = MODULE_TAG;
constant module_name = "SyncDB: Table module";

inherit Meteor.ChanelConnecteur;

#include <module.h>
inherit "module";

object table; // tæble
object schema; // schemæ

string registered_channel;

void create() {
    defvar("channel", Variable.String("control", 0, "Channel to use.",
				      "Name of the channel to request from "
				      "the controller for browser interaction."));
    defvar("sql", Variable.String("", VAR_INITIAL, "Sql server to use.",
				      "Format is mysql://user@server/database"));
    defvar("sql_table", Variable.String("", VAR_INITIAL, "Sql table to use.",
				      ""));
    defvar("syncdb_name", Variable.String("", VAR_INITIAL, "SyncDB table name",
    					  "Name this table should be refering to. Don't change it, once chosen."));

}

void start(int i, mixed conf) {
    ::start(i, conf);

    module_dependencies(conf, ({ "dbsync" }), 1);

    if (registered_channel) {
	unregister_channel(registered_channel);
	registered_channel = 0;
    }
    register_channel(query("channel"), accept);
    registered_channel = query("channel");

    if (schema) {
	string name = query("syncdb_name");
	string sql = query("sql");
	if (sizeof(sql) && sizeof(name)) {
	    Sql.Sql sql = Sql.Sql(sql, 0, 0, 0, ([ "reconnect" : 1 ]));
	    sql->set_charset("unicode");
	    table = SyncDB.Meteor.Table(name, schema, 
		    SyncDB.MySQL.Table(name, sql, schema, query("sql_table")));
	}

	conf->get_provider("syncdb")->register_table(name, table, query("channel"));
    }
}

void accept(object channel, string name) {
    channel->set_cb(table->incoming);
}

void stop() {
    if (registered_channel) {
	unregister_channel(registered_channel);
	registered_channel = 0;
    }

    ::stop();
}

