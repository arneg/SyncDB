inherit SyncDB.Table;

Meteor.Channel channel;
object in_get, in_set,
       out_set, out_get, out_update, out_error;

void create(string name, SyncDB.Schema schema, SyncDB.Table db) {
    ::create(dbname, schema, db);
    object s = Serialization.Types.String();
    in_get = Serialization.Types.Struct("_get", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    in_set = Serialization.Types.Struct("_set", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    out_get = in_get;
    out_update = Serialization.Types.Struct("_update", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    out_set = Serialization.Types.Struct("_set", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    out_error = Serialization.Types.Struct("_error", ([
	"error" : s,
	"id" : s,
    ]));
}

void generate_reply(int err, array(mapping)|mapping row, object session, mapping message) {
    if (err) {
	werror("<<< %O\n", message);
	session->send(out_error->encode(([ 
			    "id" : message->id,
			    "error" : sprintf("%O", row),
			]))->render());
	return;
    }
    if (mappingp(row)) {
	session->send(out_set->encode(([ 
			    "id" : message->id,
			    "row" : row
			]))->render());
	return;
    }
    if (arrayp(row)) {
	session->send(out_get->encode(([ 
			    "id" : message->id,
			    "row" : row[0]
			]))->render());
	return;
    }
    if (row) error("invalid type for row detected.\n");
}

void incoming(object session, Serialization.Atom a) {
    mapping message;

    // catch here and reply with error!!!
    switch (a->type) {
    case "_get": {
	message = in_get->decode(a);
	db->get(message->row, generate_reply, session, message);
	break;
    }
    case "_set":
	message = in_set->decode(a);
	db->set(message->row, generate_reply, session, message);
	break;
    }
}
