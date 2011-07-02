all: util.bloom.js util.sha256.js
%.js: %.js.in bits.h
	cpp -P $< > $@
