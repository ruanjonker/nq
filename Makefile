
.PHONY: all app clean test deps release docs

all: clean app test docs
	@echo "Done."

./ebin/nq.app: src/*/*.erl include/*.hrl test/*.erl
	./rebar compile

deps:
	./rebar get-deps

app: deps ./ebin/nq.app

./apps/nq/ebin:
	mkdir -p ./apps/nq/
	cd ./apps/nq/; rm -f ebin; ln -s ../../ebin

release: app ./apps/nq/ebin
	./rebar --force generate
	cd ./rel/nq; tar -czf ../nq.tar.gz *

deb: release ./package/deb/*.template ./package/deb/godeb.sh
	cp ./rel/nq.tar.gz ./package/deb/.
	cd ./package/deb; ./godeb.sh

docs:
	./rebar doc

clean:
	rm -f rel/nq.tar.gz
	rm -f package/deb/*.deb
	rm -f package/deb/*.tar.gz
	rm -fr package/deb/debian
	rm -fr .eunit
	rm -fr erl_crash.dump
	./rebar clean

run: app
	erl -pa ./ebin -sname nq@$(shell hostname -s)

test: app
	mkdir -p .eunit
	./rebar skip_deps=true eunit


