inherit .Base;

mapping tables;

void create(mapping tables) {
    this_program::tables = tables;
}

constant is_join = 1;
