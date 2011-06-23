all: util.bloom.js util.sha256.js
%.js: %.js.in bits.h
	cpp -P $< > $@
doc:
	sh ${JSDOCDIR}/jsrun.sh -t="${JSDOCTEMPLATEDIR}" -d=`pwd`/../docs -v -a SyncDB.js 
