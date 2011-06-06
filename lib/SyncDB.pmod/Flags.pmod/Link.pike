inherit .Base;

mapping(string:string) tables;

void create(void|mapping tables) {
    this_program::tables = tables;
}

constant is_link = 1;
