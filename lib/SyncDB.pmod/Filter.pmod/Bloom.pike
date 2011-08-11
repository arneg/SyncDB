MMP.Utils.Bloom.Filter bloom;

void create(MMP.Utils.Bloom.Filter f) {
    bloom = f;
}

int(0..1) `[](mixed idx) {
    return bloom[idx];
}

mixed `[]=(mixed idx, mixed val) {
    return bloom[idx] = val;
}
