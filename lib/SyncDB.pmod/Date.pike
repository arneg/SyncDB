Calendar.Second date;

void create(int ts) {
    date = Calendar.Second(ts);
}

string _sprintf(int fmt) {
    switch (fmt) {
    case 'd':
	return sprintf("%d", date->ux);
    case 'O':
	return sprintf("SyncDB.Date(%s)", date->format_http());
    }

    return 0;
}

mixed cast(string rtype) {
    switch (rtype) {
    case "int":
	return date->ux;
    case "string":
	return (string)date->ux; // not a good idea
    }
}

void ensure_sp(mixed other) {
    if (objectp(other) && !Program.inherits(object_program(other), this_program)) {
	error("Incomparable types: %O(%O) vs. %O(%O)\n", this_program, this,
	      object_program(other), other);
    }
}

int(0..1) `<(mixed other) {
    ensure_sp(other);
    return date->ux < other->ux;
}

int(0..1) `==(mixed other) {
    ensure_sp(other);
    return date->ux == other->ux;
}

int(0..1) `>(mixed other) {
    ensure_sp(other);
    return date->ux > other->ux;
}

int __hash() {
    return hash_value(date) ^ 4404; // make distinct from date object
}
