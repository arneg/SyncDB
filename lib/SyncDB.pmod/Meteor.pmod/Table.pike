inherit SyncDB.Table;

Meteor.Channel channel;
object in_select, in_update, in_insert,
       out_update, out_select, out_error;

void create(string name, SyncDB.Schema schema, SyncDB.Table db) {
    ::create(dbname, schema, db);
    object s = Serialization.Types.String();
    in_select = Serialization.Types.Struct("_select", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    in_update = Serialization.Types.Struct("_update", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
    in_insert = Serialization.Types.Struct("_insert", ([
	"row" : schema->parser_in(),
	"id" : s,
    ]));
    out_select = in_select;
#if 0
    out_update = Serialization.Types.Struct("_update", ([
	"row" : schema->parser_out(),
	"id" : s,
    ]));
#endif
    out_update = Serialization.Types.Struct("_update", ([
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
	session->send(out_update->encode(([ 
			    "id" : message->id,
			    "row" : row
			]))->render());
	return;
    }
    if (arrayp(row)) {
	session->send(out_select->encode(([ 
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
    case "_select": {
	message = in_select->decode(a);
	db->select(message->row, generate_reply, session, message);
	break;
    }
    case "_update":
	message = in_update->decode(a);
	db->update(message->row, generate_reply, session, message);
	break;
    case "_insert":
	message = in_insert->decode(a);
	db->insert(message->row, generate_reply, session, message);
	break;
    }
}
