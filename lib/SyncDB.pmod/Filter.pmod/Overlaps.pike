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
