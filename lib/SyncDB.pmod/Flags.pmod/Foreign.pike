inherit .Base;

constant is_foreign = 1;

string table;
string id;

void create(string table, string|void id) {
    this_program::table = table;
    this_program::id = id;
}
