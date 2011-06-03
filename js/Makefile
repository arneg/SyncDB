all: util.bloom.js util.sha256.js
%.js: %.js.in
	cpp -P $< > $@
