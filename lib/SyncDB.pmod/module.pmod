class RowBased(mapping row) {

    mixed `[](mixed key) {
	return row[key];
    }

    mixed `[]=(mixed key, mixed val) {
	return row[key] = val;
    }

    mixed `->=(mixed key, mixed val) {
	return row[key] = val;
    }

    int _mappingp() {
	return 1;
    }
}

class DeletedRow {
    inherit RowBased;
}

class pNull {
}

object Null = pNull();
