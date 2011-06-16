.Date start, stop;

void create(.Date start, .Date stop) {
    this_program::start = start;
    this_program::stop = stop;
}

int(0..1) contains(.Date supplicant) {
    return supplicant >= start && supplicant <= stop;
}
