#if constant(Meteor) 
inherit Meteor.SessionHandler;
#else
#error Cannot find Meteor library.
#endif

object table; // tæble

array(object) plexers = ({ });

void create() {
    object schema =  SyncDB.Schema(([
	"id" : SyncDB.Types.Integer(SyncDB.Flags.Key(),
				    SyncDB.Flags.Automatic()),
	"foo" : SyncDB.Types.String() 
    ]));
    table = SyncDB.Meteor.Table("ignore", schema, 
	    SyncDB.MySQL.Table("ignore", Sql.Sql("mysql://root@localhost/FOO"), schema));
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
	return "HTTP/1.1 200 OK\r\n" + Roxen.make_http_headers(headers);
}

mapping parse(Protocols.HTTP.Server.Request r) {
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
		multiplexer->get_channel("control")->set_cb(table->incoming);
		// TODO:: inform table of new connection
		plexers += ({ multiplexer });

		write("new session created just now. %O\n", sessions);

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
		call_out(session->handle_id, 0, id);
		return Roxen.http_pipe_in_progress();
	}

	werror("unknown session '%O'(%O,%O) in %O\n", id->variables["id"], id->variables, r->variables, sessions);
	answer(r, 500, "me dont know you");
	return Roxen.http_pipe_in_progress();
} 
