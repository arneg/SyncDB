class Boundary(mixed x) { 
    int (0..1) `<(object b) {
	if (!objectp(b) || !Program.inherits(object_program(b), Boundary)) {
	    return x < b;
	}
	return x < b->x;
    }

    int (0..1) `>(object b) {
	if (!objectp(b) || !Program.inherits(object_program(b), Boundary)) {
	    return x > b;
	}
	return x > b->x;
    }

    int unix_time() {
	return x->unix_time();
    }

    int `ux() {
	return unix_time();
    }

    string _sprintf(int type) {
	return sprintf("%O", x);
    }
}

class Open {
    inherit Boundary;

    int (0..1) `==(object b) {
	return b->x == x && Program.inherits(object_program(b), Open);
    }

    int (0..1) `<(mixed b) {
	if (!objectp(b) || !Program.inherits(object_program(b), Boundary)) {
	    return x <= b;
	}
	return ::`<(b);
    }

    int (0..1) `>(mixed b) {
	if (!objectp(b) || !Program.inherits(object_program(b), Boundary)) {
	    return x >= b;
	}
	return ::`>(b);
    }

    int(0..1) overlaps(object b) {
	if (this > b) return 1;
	return 0;
    }

    int(0..1) touches(object b) {
	werror("%O touches %O == %d\n", this, b, overlaps(b) || (this == b && Program.inherits(object_program(b), Closed)));
	return overlaps(b) || (this == b && Program.inherits(object_program(b), Closed));
    }

    string _sprintf(int type, mapping info) {
	if (info->flag_left) {
	    return sprintf("(%O", x);
	}
	return sprintf("%O)", x);
    }

    Boundary `~() {
	return Closed(x);
    }
}

class Closed {
    inherit Boundary;

    int (0..1) `==(object b) {
	return Program.inherits(object_program(b), Closed) && b->x == x;
    }

    int(0..1) overlaps(object b) {
	if (this > b || (Program.inherits(object_program(b), this_program) && b == this)) return 1;
	return 0;
    }

    int(0..1) touches(object b) {
	werror("%O touches %O == %d\n", this, b, this > b);
	if (this < b) return 0;
	return 1;
    }
    string _sprintf(int type, mapping info) {
	if (info->flag_left) {
	    return sprintf("[%O", x);
	}
	return sprintf("%O]", x);
    }

    Boundary `~() {
	return Open(x);
    }
}

Boundary min(Boundary a, Boundary b) {
    if (a < b) return a;
    if (b < a) return b;

    if (Program.inherits(object_program(a), Closed)) return a;
    if (Program.inherits(object_program(b), Closed)) return b;
    return a;
}

Boundary max(Boundary a, Boundary b) {
    if (a == min(a,b)) return b;
    else return a;
}

Boundary a, b;

mixed `->start() { return a->x; }
mixed `->stop() { return b->x; }

mixed `->start=(mixed v) {
    a = Closed(v);
    return v;
}
mixed `->stop=(mixed v) {
    b = Closed(v);
    return v;
}

string _sprintf(int type) {
    return sprintf("%-O..%O", a, b);
}

void create(mixed a, mixed b) {
    if (!objectp(a) || !Program.inherits(object_program(a), Boundary)) {
	a = Closed(a);
    }
    if (!objectp(b) || !Program.inherits(object_program(b), Boundary)) {
	b = Closed(b);
    }
    if (!b->overlaps(a)) error("Trying to create empty interval.\n");
    this_program::a = a;
    this_program::b = b;
}

int(0..1) `==(mixed i) {
    return objectp(i) && Program.inherits(object_program(i), this_program) && a == i->a && b == i->b;
}

//  0	(..)..[..]
//  1	(..[..)..]
//  2	[..(..)..]
//  3	[..(..]..)
//  4	[..]..(..)

this_program `&(this_program i) {
    Boundary l, r;

    l = max(a, i->a);
    r = min(b, i->b);

    mixed e = catch {
	if (r->overlaps(l))
	    return this_program(l, r);
    };
    if (e) {
	werror(">> %O(%O)->overlaps(%O(%O)).\n", r, objectp(r) && object_program(r),
	       l, objectp(l) && object_program(l));
	error(e);
    }
    return 0;
}

int(0..1) overlaps(this_program i) {
    Boundary l, r;

    l = max(a, i->a);
    r = min(b, i->b);

    return r->overlaps(l);
}

this_program `|(this_program i) {
    if ((this & i) 
    || (b <= i->a && b->touches(i->a))
    || (i->b <= a && i->b->touches(a))) {
	return this_program(min(a, i->a), max(b, i->b));
    }

    error("%O and %O need to overlap.\n", this, i);
}

this_program `+(this_program i) {
    return this | i;
}

this_program `-(this_program interval) {
    this_program i = interval & this;
    if (i) {
	if (i == this) return 0;

	if (a == i->a) {
	    return this_program(~i->b, b);
	} else if (b == i->b) {
	    return this_program(a, ~i->a);
	}

	error("%O and %O may not be contained.\n", this, i);
    }
    return this;
}

int(0..1) contains(mixed x) {
    if (!objectp(x) || !Program.inherits(object_program(x), Boundary)) {
	x = this_program(Closed(x), Closed(x));
    }
    return !!(this&x);
}

/* TODO:
 * implement or remap the api offered by the timerange thing.
 */

mixed beginning() { return start; }
mixed end() { return stop; }

mixed cast(string type) {
    switch (type) {
    case "array":
	return ({ start, stop });
    }
}
