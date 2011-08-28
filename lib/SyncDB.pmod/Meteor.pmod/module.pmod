class Sync(void|string id, void|array(mapping) rows) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %d rows)", this_program, id, sizeof(rows));
    }
}
class SyncReq(void|string id, SyncDB.Version version, mapping|void filter) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, filter);
    }
}
class Update(void|string id, SyncDB.Version version, mapping row, mixed key) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, row);
    }
}
class Error(void|string id, void|string error) { 
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, error);
    }
}
class Base(void|string id, void|mapping row) { 
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, row);
    }
}
class Select(void|string id, object filter) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, filter);
    }
}
class Reply(string id, array(mapping) rows) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %d rows)", this_program, id, sizeof(rows));
    }
}
class Insert { inherit Base; }


