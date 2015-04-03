inherit .Migration;

mapping transform_row(mapping row) {
    return row;
}

this_program `+(this_program ... b) {
    if (sizeof(map(b, object_program) - ({ this_program })))
        error("Bad argument.\n");
    return this;
}
