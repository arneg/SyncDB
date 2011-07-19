inherit SyncDB.Table;

object in, out;

mapping blacklist = ([ ]);

void create(string name, SyncDB.Schema schema, SyncDB.Table db) {
    ::create(dbname, schema, db);
    object s = Serialization.Types.String();
    object i = Serialization.Types.Int();

    in = Serialization.Types.Polymorphic();
    in->register_type(.Select, "_select", 
		      Serialization.Types.Struct("_select", ([
			    "filter" : SyncDB.Serialization.Filter,
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
    in->register_type(.SyncReq, "_syncreq",
			Serialization.Types.Struct("_syncreq", ([
			    "version" : Serialization.Types.OneTypedList(i),
			    "id" : s,
			    //"bloom" : ..,
			])));

    out = Serialization.Types.Polymorphic();
    out->register_type(.Reply, "_reply", 
		      Serialization.Types.Struct("_reply", ([
			    "rows" : Serialization.Types.OneTypedList(
					schema->parser_out()),
			    "id" : s,
			]), .Reply));
    out->register_type(.Sync, "_sync",
		       Serialization.Types.Struct("_sync", ([
			    "rows" : Serialization.Types.OneTypedList(
					    schema->parser_out()),
			    "version" : Serialization.Types.OneTypedList(i),
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

void generate_reply(int err, array(mapping)|mapping row, object session, object message, void|SyncDB.Version version) {
    object reply;
    if (err) {
	werror("<<< %O\n", message);

	reply = .Error(message->id, sprintf("%O", row));
    } else switch (object_program(message)) {
    case .Select:
	if (arrayp(row)) {
	    reply = .Reply(message->id, row);
	    break;
	} else error("invalid type for row detected.\n");
    case .Update:
    case .Insert:
	if (!mappingp(row))
	    error("Bad return type from db: %O\nexpected mapping.\n", row); 
	reply = .Sync(message->id, (array) (version||row->version), ({ row }));
	// send
	if (object_program(message) != .SyncReq && sizeof(sessions)) {
	    blacklist[row->version] = 1;
	    string s = out->encode(.Sync("", (array)row->version, ({ row })))->render();
	    werror("sending update to %d clients\n", sizeof(sessions));
	    foreach (sessions; object o;) {
		if (o == session) continue;
		// check bloom filter or shit like that
		o->send(s);
	    }
	}
	// to all others
	break;
    default:
	error("Unknown message type: %O\n", message);
    }
    session->send(out->encode(reply)->render());
}

// get triggered by e.g. MysqlTable
void generate_sync(int err, SyncDB.Version version, array(mapping) rows) {
    if (m_delete(blacklist, version)) return;

    if (sizeof(sessions)) {
	string s = out->encode(.Sync("", (array)version, rows))->render();
	foreach (sessions; object o;) {
	    // check bloom filter or shit like that
	    o->send(s);
	}
    }
}

mapping sessions = set_weak_flag(([ ]), Pike.WEAK_INDICES);

void incoming(object session, Serialization.Atom a) {
    werror("TABLE: incoming(%O, %O)\n", session, a);
    object message = in->decode(a);
    werror("TABLE: decoded to %O\n", message);

    switch (object_program(message)) {
    case .Select:
	werror("TABLE: select(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->select(message->filter, generate_reply, session, message);
	break;
    case .Update:
	werror("TABLE: update(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->update(message->row, generate_reply, session, message);
	break;
    case .Insert:
	werror("TABLE: insert(%O, %O, %O, %O) (%O)\n", message->row, generate_reply, session, message, object_program(message));
	db->insert(message->row, generate_reply, session, message);
	break;
    case .SyncReq:
	werror("TABLE: syncreq(%O, %O, %O) (%O)\n", message->version, generate_reply, session, message);
	SyncDB.Version v = SyncDB.Version(message->version);
	sessions[session] = v;
	db->syncreq(v, generate_reply, session, message);
    default:
	error("Unknown message type: %O\n", message);
    }
}
