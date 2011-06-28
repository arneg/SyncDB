#if constant(Meteor) 
inherit Meteor.SessionHandler;
#else
#error Cannot find Meteor library.
#endif

object table; // tæble

array(object) plexers = ({ });

void create() {
    object schema = SyncDB.Schema(
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
	    SyncDB.Types.Date("stopdate", Calendar.Second)
	),
	// two
	SyncDB.Types.String("firstname", SyncDB.Flags.Foreign("two", "firstname")),
	SyncDB.Types.String("lastname", SyncDB.Flags.Foreign("two", "lastname")),
    );
    table = SyncDB.Meteor.Table("one", schema, 
	    SyncDB.MySQL.Table("one", Sql.Sql("mysql://root@localhost/interSync"), schema, "one"));
}

void answer(object r, int code, string data) {
	r->send_result(Roxen.http_low_answer(code, data));
}

function combine(function f1, function f2) {
	mixed f(mixed ...args) {
		return f1(f2(@args));
	};

	return f;
}

string make_response_headers(mapping headers) {
	headers["Cache-Control"] = "no-cache";
	return "HTTP/1.1 200 OK\r\n" + Roxen.make_http_headers(headers);
}

mapping parse(Protocols.HTTP.Server.Request r) {
	write("new request: %O\n", r);
	string f = basename(r->not_query);
#if 1
	mapping id = ([
		"variables" : r->variables,
		"answer" : combine(r->send_result, Roxen.http_low_answer),
		"end" : r->end,
		"method" : r->method,
		"request_headers" : r->request_headers,
		"misc" : ([ 
			"content_type_type" : r->misc["content_type_type"],
		]),
		"make_response_headers" : make_response_headers,
		"connection" : r->connection,
		"data" : r->data,
	]);
#else
	object id = r;
#endif

	object session;

	if (id->method == "GET" && !has_index(id->variables, "id")) {
		session = get_new_session();
		object multiplexer = Meteor.Multiplexer(session);
#ifdef DB_SERVER_DEBUG
		if (!table->incoming) {
		    werror("!incoming: %O, %O\n", table, table->incoming);
		}
#endif
		multiplexer->get_channel("control")->set_cb(table->incoming);
		// TODO:: inform table of new connection
		//plexers += ({ multiplexer });

		write("new session %O created just now.\n", session->client_id);

		string response = sprintf("_id %s",
					  Serialization.Atom("_string", session->client_id)->render());

		return ([
			"data" : Serialization.Atom("_vars", response)->render(),
			"type" : "text/atom",
			"error" : 200,
			"extra_heads" : ([
				"Cache-Control" : "no-cache",
			]),
		]);
	}

	// we should check whether or not this is hitting a max connections limit somewhere.
	if ((session = sessions[id->variables["id"]])) {
		werror("SERVER: call_out(handle_id, 0, id)\n");
		call_out(session->handle_id, 0, id);
		return Roxen.http_pipe_in_progress();
	}

	werror("unknown session '%O'(%O,%O) in %O\n", id->variables["id"], id->variables, r->variables, sessions);
	return ([
		"data" : "me dont know you",
		"type" : "text/plain",
		"error" : 500,
		"extra_heads" : ([
		    "Connection" : "keep-alive"
		])
	]);
} 
