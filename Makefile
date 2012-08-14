all:
	@rebar get-deps compile

edoc:
	@rebar doc

check:
	@rm -rf .eunit
	@mkdir -p .eunit
	@dialyzer -Wno_opaque --src src
	@rebar eunit

clean:
	@rebar clean

maintainer-clean:
	@rebar clean
	@rebar delete-deps
	@rm -rf deps
