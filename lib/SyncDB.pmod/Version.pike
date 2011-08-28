array(int) a;

void create(array(int)|int n) {
    if (n)
	a = intp(n) && allocate(n) || n;
    else error("evil!");
}

#if 0
int __hash() {
    if (!sizeof(a)) return 0;
    return hash_value(`+(@a));
}
#endif

int(0..1) `==(mixed o) {
    return (objectp(o) && arrayp(o->a) && equal(a, o->a));
}

int `[](mixed idx) {
    return a[idx];
}

int `[]=(mixed idx, mixed val) {
    a[idx] = val;
    return val;
}

int `<(mixed o) {
    if (objectp(o) && arrayp(o->a) && sizeof(o->a) == sizeof(a)) {
	array t = o->a[*] - a[*];
	return (min(@t) > 0 && max(@t) > 0);
    }
    return 0;
}

int `>(mixed o) {
    if (objectp(o) && arrayp(o->a) && sizeof(o->a) == sizeof(a)) {
	array t = a[*] - o->a[*];
	return (min(@t) > 0 && max(@t) > 0);
    }
    return 0;
}

string _sprintf(int fmt) {
    switch (fmt) {
    case 'd':
	return (array(string))a * ".";
    case 'O':
	return sprintf("SyncDB.Version(%O)", a);
    }
    return 0;
}

mixed cast(string type) {
    if (type == "array") {
	return a;
    } else error("Cannot cast %O into %s\n", this, type);
}

int _sizeof() {
    return sizeof(a);
}
