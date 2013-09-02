inherit .Base;

constant is_maxlength = 1;

int length;

void create(int len) {
    length = len;
}

mixed cast(string s) {
    if (s == "int") return length;
    error("Cannot cast %O to %O.\n", this, s);
}
