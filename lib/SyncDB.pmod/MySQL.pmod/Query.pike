string fmt;
array args;

protected void create(string fmt, mixed ... args) {
    switch (sizeof(args)) {
    case 1:
	if (arrayp(args[0])) {
	    args = args[0];
	    this_program::fmt = fmt;
	    this_program::args = ({ });
	    map(args, `+=);
	    return;
	}
	break;
    case 2:
	if (arrayp(args[0]) && stringp(args[1])) {
	    this_program::fmt = fmt;
	    this_program::args = ({ });
	    string s = args[1];
	    foreach (args[0]; int i; mixed v) {
		if (i) `+=(s);
		`+=(v);
	    }
	    return;
	}
	break;
    case 3:
	if (mappingp(args[0]) && stringp(args[1]) && stringp(args[2])) {
	    array a = allocate(sizeof(args[0]));
	    int i = 0;
	    foreach (args[0]; string field; mixed v) {
		if (i) fmt += args[2];
		fmt += sprintf("%s %s %%s", field, args[1]);
		a[i++] = v;
	    }
	    args = a;
	}
	break;
    } 
    this_program::fmt = fmt;
    this_program::args = args;
}

void add(string fmt, mixed ... args) {
    this_program::fmt += fmt;
    this_program::args += args;
}

//#define DB_DEBUG
protected mixed `()(Sql.Sql sql) {
#ifdef DB_DEBUG
    array res;

    int t = gethrtime();

    res = sql->query(fmt, @args);

    werror("SQL(%2f ms): %O\n", (gethrtime() - t) / 1000.0, this);
    return res;
#else
    return sql->query(fmt, @args);
#endif
}

protected mixed `+(mixed ... list) {
    this_program ret;
    mixed b = list[0];

    if (stringp(b)) {
	ret = this_program(fmt+b, @args);
    } else if (arrayp(b)) {
	ret = this_program(fmt, @(args + b));
    } else if (objectp(b) && Program.inherits(b, this_program)) {
	ret = this_program(fmt + b->fmt, @(args + b->args));
    } else error("Bad argument to `+: %O\n", b);

    foreach (list[1..];; mixed v) {
	ret += v;
    }
    return ret;
}

protected mixed ``+(mixed b) {
    if (stringp(b)) {
	return this_program(b + fmt, @args);
    } else if (arrayp(b)) {
	return this_program(fmt, @(b + args));
    } else error("Bad argument to ``+: %O\n", b);
}

protected mixed `+=(mixed ... list) {
    string s = fmt;
    array a = args;

    foreach (list;; mixed b) {
	if (stringp(b)) {
	    s += b;
	} else if (arrayp(b)) {
	    a += b;
	} else if (objectp(b)) {
	    s += b->fmt;
	    a += b->args;
	} else {
	    error("cannot add %O\n", b);
	}
    }

    fmt = s;
    args = a;
    return this;
}

protected string _sprintf(int t) {
    return sprintf("%O(%O, %O)", this_program, fmt, (args));
}

protected int _sizeof() {
    return sizeof(fmt);
}

string render(function quote) {
    return sprintf(fmt, @map(args, quote));
}
