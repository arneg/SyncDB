array a;

int num_rows;

void create(array a) {
    this_program::a = a;
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
