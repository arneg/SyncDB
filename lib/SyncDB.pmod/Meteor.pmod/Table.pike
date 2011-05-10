inherit SyncDB.Table;

Meteor.Channel channel;
object in, out;

void create(string name, SyncDB.Schema schema, SyncDB.Table db) {
    ::create(dbname, schema, db);
    object s = Serialization.Types.String();
    object i = Serialization.Types.Int();

    in = Serialization.Types.Polymorphic();
    in->register_type(.Select, "_select", 
		      Serialization.Types.Struct("_select", ([
			    "row" : schema->parser_out(),
			    "id" : s,
			]), .Select));
    in->register_type(.Update, "_update",
		      Serialization.Types.Struct("_update", ([
			    "row" : schema->parser_out(),
			    "id" : s,
			]), .Update));
    in->register_type(.Insert, "_insert",
		      Serialization.Types.Struct("_insert", ([
			    "row" : schema->parser_in(),
			    "id" : s,
			]), .Insert));
    out = Serialization.Types.Polymorphic();
    out->register_type(.Select, "_select", 
		      Serialization.Types.Struct("_select", ([
			    "row" : schema->parser_out(),
			    "id" : s,
			]), .Select));
    out->register_type(.Sync, "_sync",
		       Serialization.Types.Struct("_sync", ([
			    "row" : schema->parser_out(),
			    "id" : s,
			]), .Sync));
    // we probably dont need this one. _sync is the new
    // reply type for insert/update/delete
    out->register_type(.Update, "_update",
		       Serialization.Types.Struct("_update", ([
			    "row" : schema->parser_out(),
			    "id" : s,
			]), .Update));
    out->register_type(.Error, "_error",
		       Serialization.Types.Struct("_error", ([
			    "error" : s,
			    "id" : s,
			]), .Error));
}

void generate_reply(int err, array(mapping)|mapping row, object session, object message) {
    object reply;
    if (err) {
	werror("<<< %O\n", message);

	reply = .Error(message->id, sprintf("%O", row));
    } else switch (object_program(message)) {
    case .Select:
	if (arrayp(row) && sizeof(row)) {
	    if (sizeof(row) > 1) 
		werror("WARN: selecting several rows, while only one is"
		       " supported.\n");
	    reply = .Select(message->id, row[0]);
	    break;
	} else error("invalid type for row detected.\n");
    case .Update:
    case .Insert:
	if (!mappingp(row))
	    error("Bad return type from db: %O\nexpected mapping.\n", row); 
	reply = .Sync(message->id, row);
	break;
    default:
	error("Unknown message type: %O\n", message);
    }
    session->send(out->encode(reply)->render());
}

void incoming(object session, Serialization.Atom a) {
    werror("TABLE: incoming(%O, %O)\n", session, a);
    object message = in->decode(a);
    werror("TABLE: decoded to %O\n", message);

    switch (object_program(message)) {
    case .Select:
	werror("TABLE: select(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->select(message->row, generate_reply, session, message);
	break;
    case .Update:
	werror("TABLE: update(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->update(message->row, generate_reply, session, message);
	break;
    case .Insert:
	werror("TABLE: insert(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->insert(message->row, generate_reply, session, message);
	break;
    }
}
