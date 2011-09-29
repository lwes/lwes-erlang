all:
	@rebar get-deps compile

edoc:
	@rebar skip_deps=true doc

check:
	@rm -rf .eunit
	@mkdir -p .eunit
	@dialyzer -Wno_opaque --src src
	@rebar skip_deps=true eunit

clean:
	@rebar clean

maintainer-clean:
	@rebar clean
	@rebar delete-deps
	@rm -rf deps
