inherit Serialization.BasicTypes;
inherit SyncDB.Table;

object in, out;
object type_cache = Serialization.default_type_cache;

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
    mapping m = ([]);
    foreach (schema->m; string name; object type) {
	if (type->is_index) {
	    m[name] = type->get_filter_parser();
	}
    }
    object l = Mapping(UTF8String(), Serialization.Types.Or(@values(m)));
    in->register_type(.SyncReq, "_syncreq",
			Serialization.Types.Struct("_syncreq", ([
			    "version" : Serialization.Types.OneTypedList(i),
			    "id" : s,
			    "filter" : l
			]), .SyncReq));

    out = Serialization.Types.Polymorphic();
    out->register_type(.Reply, "_reply", 
		      Serialization.Types.Struct("_reply", ([
			    "rows" : Serialization.Types.OneTypedList(
					schema->parser_out()),
			    "id" : s,
			]), .Reply)),
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
	// TODO we maybe dont want to send out the desribe error here. it might contain things we dont want
	// to let the client know (sql passwords in worst case)

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

	generate_sync(0, version||row->version, ({ row }));
	// to all others
	break;
    case .SyncReq:
	reply = .Sync(message->id, (array)version, row);
	break;
	werror("no support for %O, yet.\n", message);
	break;
    default:
	error("Unknown message type: %O\n", message);
    }
    session->send(out->encode(reply)->render());
}

mapping(string:mapping(object:object)) filters = ([]);

// get triggered by e.g. MysqlTable
void generate_sync(int err, SyncDB.Version version, array(mapping) rows) {
    if (m_delete(blacklist, version)) return;

    mapping updates = ([]);

    if (sizeof(filters)) {
	foreach (rows;; mapping row) {
	    foreach (row; string name; mixed o) {
		mapping f = filters[name];
		if (!f) continue;
		mixed h;
		foreach (f; object session; object filter) {
		    if (h) {
			if (!h(filter)) continue;
		    } else {
			if (filter->prepare) {
			    h = filter->prepare(o);
			    if (!h(filter)) continue;
			} else if (!filter->has(o)) continue;
		    }

		    if (has_index(updates, session)) updates[session] += ({ row });
		    else updates[session] = ({ row });
		}
	    }
	}
	// CUBE!
	foreach (updates; object o; array(mapping) r) {
	    string s = out->encode(.Sync("", (array)version, r))->render();
	    catch {
		o->send(s);
	    };
	}
    }
}

void incoming(object session, Serialization.Atom a) {
    object message = in->decode(a);

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

	foreach (message->filter; string name; object f) {
	    if (!filters[name]) filters[name] = set_weak_flag(([ ]), Pike.WEAK_INDICES);
	    filters[name][session] = f;
	}
	// check version and trigger update based on filter
	db->syncreq(v, message->filter, generate_reply, session, message);
	break;
    default:
	error("Unknown message type: %O\n", message);
    }
}
