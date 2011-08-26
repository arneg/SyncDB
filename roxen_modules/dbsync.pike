constant MODULE_STEALTH = 0;

constant module_type = MODULE_TAG | MODULE_PROVIDER;
constant module_name = "SyncDB: Server module";

#include <module.h>
inherit "module";

string query_provides() {
    return "syncdb";
}

mapping(string:object) tables = ([]);
mapping(string:string) channels = ([]);

void register_table(string name, object table, string channel) {
    tables[name] = table;
    channels[name] = channel;
}

string simpletag_schema(string tagname, mapping args, string content,
			RequestID id) {
    if (!args->name || !tables[args->name]) error("No such table: %O in %O", args->name, tables);
    return Standards.JSON.encode(tables[args->name]->schema);
}

string simpletag_channel(string tagname, mapping args, string content,
			RequestID id) {
    if (!args->name || !channels[args->name]) error("No such table: %O in %O", args->name, channels);
    return Standards.JSON.encode(channels[args->name]);
}
