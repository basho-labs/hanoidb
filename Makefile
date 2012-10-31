TARGET=		hanoidb

REBAR=		rebar
DIALYZER=	dialyzer


.PHONY: plt analyze all deps compile get-deps clean

all: compile

deps: get-deps

get-deps:
	@$(REBAR) get-deps

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

test: eunit

eunit: compile clean-test-btrees
	@$(REBAR) eunit skip_deps=true

eunit_console:
	erl -pa .eunit deps/*/ebin

clean-test-btrees:
	rm -fr .eunit/Btree_* .eunit/simple

plt: compile
	$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa deps/*/ebin --apps kernel stdlib

analyze:
	$(DIALYZER) --plt .$(TARGET).plt -pa deps/*/ebin ebin

repl:
	erl -pz deps/*/ebin -pa ebin
