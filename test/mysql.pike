inherit "base";

object schema, table;

void cb(int err, mixed v) {
    if (err) {
	werror("failed: %O\n", v);
    } else {
	werror(">> %O\n", v);
    }
}

void test_equal(mixed a, mixed b) {
    if (a != b) {
	error("%O vs %O after insert.\n", a, b);
    }
}

void test_update(mapping m) {
    string gecos = random_string(20);
    string name = random_string(10);
    string email = sprintf("%s@%s.com", random_string(4), random_string(5));
    mapping row = ([
	"name" : name,
	"password" : "b%%%%lub",
	"email" : email,
	"gecos" : gecos,
	"uid" : m->uid,
    ]);

    void update_cb(int err, mixed v) {
	if (err) throw(v);
	//werror("updated %O\n", v);
    };
    
    table->update(row, m->version, update_cb);
}

void test_random() {
    string gecos = random_string(20);
    string name = random_string(10);
    string email = sprintf("%s@%s.com", random_string(4), random_string(5));

    void insert_cb(int err, mixed row) {
	if (err) throw(row);

	void select_cb(int err, mixed row) {
	    if (err) throw(row);

	    if (sizeof(row) != 1)
		error("bad number of rows: %d\n", sizeof(row));
	    row = row[0];

	    test_equal(name, row->name);
	    test_equal(email, row->email);
	    test_equal(gecos, row->gecos);
	    test_update(row);
	};

	table->select(schema["name"]->Equal(name), select_cb);
    };

    table->insert(([
	"name" : name,
	"password" : "b%%%%lub",
	"email" : email,
	"gecos" : gecos,
    ]), insert_cb);
}

int main(int argc, array(string) argv) {
    sql_path = argv[-1];

    schema = SyncDB.Schema(
	SyncDB.Types.Integer("uid", SyncDB.Flags.Key(), SyncDB.Flags.Automatic()),
	SyncDB.Types.String("name", 255, SyncDB.Flags.Unique(), SyncDB.Flags.Index()),
	SyncDB.Types.String("password", 255),
	SyncDB.Types.String("email", 255, SyncDB.Flags.Unique(), SyncDB.Flags.Index()),
	SyncDB.Types.String("gecos")
    );

    SyncDB.MySQL.create_table(sql, "test", schema);

    sql->query("DELETE from test");

    table = SyncDB.MySQL.Table("test", `sql, schema, "test");

    table->select(schema["name"]->Equal("b%%ar"), cb);

    int i;

    for (i = 0; i < 1000; i++)
	test_random();

    Debug.Profiling.display();
    return 0;
}
