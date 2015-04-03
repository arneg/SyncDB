array a;

int num_rows;

void create(array a, void|int num_rows) {
    this_program::a = a;
    this_program::num_rows = num_rows;
}

mixed `[](int i) {
    return a[i];
}

mixed `[]=(int i, mixed v) {
    return a[i] = v;
}

int _sizeof() {
    return sizeof(a);
}

object _get_iterator() {
    return get_iterator(a);
}

void sort(function cmp, mixed ... args) {
    a = Array.sort_array(a, cmp, @args);
}

mixed cast(string type) {
    if (type == "array") return a;
    error("Cannot cast %O to %O\n", this, type);
}
