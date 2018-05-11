all:
	@rebar get-deps compile

edoc:
	@rebar doc

check:
	@rm -rf .eunit
	@mkdir -p .eunit
	@dialyzer --src src | \
           fgrep -vf .dialyzer-ignore-warnings
	@rebar eunit

clean:
	@rebar clean

maintainer-clean:
	@rebar clean
	@rebar delete-deps
	@rm -rf deps
