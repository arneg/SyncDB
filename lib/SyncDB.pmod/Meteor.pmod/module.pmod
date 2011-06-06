class Sync(void|string id, void|array(int) version, void|array(mapping) rows) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, error);
    }
}
class SyncReq(void|string id, void|array(int) version) {
    string _sprintf(int type) {
	return sprintf("%O(%O, %O)", this_program, id, error);
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
class Insert { inherit Base; }
class Select { inherit Base; }
class Update { inherit Base; }

