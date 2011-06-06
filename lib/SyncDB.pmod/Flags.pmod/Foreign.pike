inherit .Base;

constant is_foreign = 1;

string table;
string field;

void create(void|string table, string|void field) {
    this_program::table = table;
    this_program::field = field;
}
