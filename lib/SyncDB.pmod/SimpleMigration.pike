inherit .Migration;

mapping transform_row(mapping row) {
    return row;
}

.MySQL.Query upgrade_table() {
    // These transformation are actually not going to be needed, we are
    // creating a new table anyway, reinserting everything.
    return 0;
}

this_program `+(this_program ... b) {
    if (sizeof(map(b, object_program) - ({ this_program })))
        error("Bad argument.\n");
    return this;
}
