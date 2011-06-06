array(int) a;

void create(int n) {
    a = allocate(n);
}

int __hash() {
    return hash_value(`+(@a));
}

int(0..1) `==(mixed other) {
    return (objectp(other) && arrayp(other->a) && equal(a, other->a));
}

int `[](mixed idx) {
    return a[idx];
}

int `[]=(mixed idx, mixed val) {
    a[idx] = val;
    return val;
}

string _sprintf(int fmt) {
    switch (fmt) {
    case 'd':
	return (array(string))a * ".";
    case 'O':
	return sprintf("SyncDB.Version(%d)", this);
    }
    return 0;
}
