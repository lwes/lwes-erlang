NAME=lwes

REBAR3=rebar3
all:
	  $(REBAR3) compile

dialyzer:
	$(REBAR3) dialyzer

test: dialyzer
	  $(REBAR3) as test do eunit,cover

# Compile and run unit test for individual modules: 'make test-oxgw_util'
# or 'make test-oxgw_util test-oxgw_request'
test-%: src/%.erl
	$(REBAR3) as test do eunit -m $*

name:
	@echo $(NAME)

version:
	@echo $(shell awk 'match($$0, /[0-9]+\.[0-9]+(\.[0-9]+)+/){print substr($$0, RSTART,RLENGTH); exit}' ChangeLog)

clean:
	if test -d _build; then $(REBAR3) clean; fi

maintainer-clean: clean
	rm -rf _build

.PHONY:  all test name version clean maintainer-clean
