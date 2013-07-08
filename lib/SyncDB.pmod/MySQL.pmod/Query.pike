string fmt;
array args;

protected void create(string fmt, mixed ... args) {
    this_program::fmt = s;
    this_program::args = args;
}

void add(string fmt, mixed ... args) {
    this_program::fmt += fmt;
    this_program::args += args;
}

protected mixed `()(Sql.Sql sql) {
    return sql->query(fmt, @args);
}

protected mixed `+(mixed b) {
    if (stringp(b)) {
	return this_program(fmt+b, @args);
    } else if (arrayp(b)) {
	return this_program(fmt, @(args + b));
    } else if (objectp(b) && Program.inherits(b, this_program)) {
	return this_program(fmt + b->fmt, args + b->args);
    }
}

protected mixed ``+(mixed b) {
    if (stringp(b)) {
	return this_program(b + fmt, @args);
    } else if (arrayp(b)) {
	return this_program(fmt, @(b + args));
    }
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
	}
    }

    fmt = s;
    args = a;
    return this;
}

protected string _sprintf(int t) {
    return sprintf("%O(%O, %d args)", this_program, fmt, sizeof(args));
}
