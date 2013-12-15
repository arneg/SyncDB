object table, restriction;

void create(object table, object filter) {
    this_program::table = table;
    this_program::restriction = filter;
}


void select_complex(object filter, object order, object limit, mixed cb, mixed ... extra) {
    table->select_complex(filter & restriction, order, limit, cb, @extra);
}

void select(object filter, object|function(int(0..1), array(mapping)|mixed:void) cb, mixed ... extra) {
    table->select(filter & restriction, cb, @extra);
}

void insert(mapping row, function(int(0..1),mixed,mixed...:void) cb2, mixed ... extra) {
    row += ([]);
    restriction->insert(row);
    table->insert(row, cb2, extra);
}