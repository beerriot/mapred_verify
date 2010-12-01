all: escriptize

escriptize: compile
	./rebar escriptize

compile:
	./rebar compile

clean:
	./rebar clean
	rm -f ./mapred_verify