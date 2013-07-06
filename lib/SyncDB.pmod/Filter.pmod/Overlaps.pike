#if !constant(ADT.CriBit)
constant this_program_does_not_exist = 1;
#else
ADT.CritBit.RangeSet rangefilter;

void create(ADT.CritBit.RangeSet o) {
    rangefilter = o;
}

mixed `[](mixed key) {
    return rangefilter->overlaps(key);
}

mixed `[]=(mixed key, mixed val) {
    return rangefilter[key] = val;
}
#endif
