%%% ----------------------------------------------------------------------------
%%%     @title          Hibari operations handling unit
%%%     @author         Ilya Ponetayev <Ilya.Ponetaev@kodep.ru>
%%%     @version        0.1
%%%     Part of celestial library
%%% ----------------------------------------------------------------------------
-module(hibari).

-vsn(0.1).

-compile(export_all).

-include("db_reply.hrl").
-include("db_config.hrl").


make_op(Op, Key, Value, TS, Exptime, Flags) ->
	{Op, term_to_binary(Key), TS, term_to_binary(Value), Exptime, Flags}.



make_add(Key, Value, TS, Exptime, Flags) ->
	make_op(add, Key, Value, TS, Exptime, Flags).

make_add(Key, Value, TS, Flags) ->
	make_add(Key, Value, TS, 0, Flags).

make_add(Key, Value, TS) ->
	make_add(Key, Value, TS, []).

make_add(Key, Value) ->
	make_add(Key, Value, lib:make_ts()).



make_set(Key, Value, TS, Exptime, Flags) ->
	make_op(set, Key, Value, TS, Exptime, Flags).

make_set(Key, Value, TS, Flags) ->
	make_set(Key, Value, TS, 0, Flags).

make_set(Key, Value, TS) ->
	make_set(Key, Value, TS, []).

make_set(Key, Value) ->
	make_set(Key, Value, lib:make_ts()).


make_txn(Table, Ops, Timeout, Flags) when is_list(Ops) ->
	{do, Table, [txn | Ops], Flags, Timeout}.

make_txn(Table, Ops, Flags) ->
	make_txn(Table, Ops, 5000, Flags).

make_txn(Table, Ops) ->
	make_txn(Table, Ops, []).

%% -----------------------------------------------------------------------------
%% Hibari operations
%% -----------------------------------------------------------------------------
do_set(Key, Value, Cfg) ->
	do_hibari_cmd(set, Key, Value, Cfg).

do_add(Key, Value, Cfg) ->
	do_hibari_cmd(add, Key, Value, Cfg).

do_replace(Key, Value, Cfg) ->
	do_hibari_cmd(replace, Key, Value, Cfg).

do_hibari_cmd(Cmd, Key, Value, #db_config{table = Table,
	timeout = Timeout, flags = Flags, exptime = Exptime} = Cfg) ->
	pass_hibari_request(
		{Cmd,
			Table,
			term_to_binary(Key),
			term_to_binary(Value),
			Exptime, Flags, Timeout},
		fun () ->
			?R_OK end,
		Cfg).

do_get(Key, #db_config{table = Table, timeout = Timeout,
	flags = Flags} = Cfg) ->
	pass_hibari_request(
		{get,
			Table,
			term_to_binary(Key),
			Flags, Timeout},
		fun
			(X, _TS) when is_binary(X) ->
				binary_to_term(X);
			(undefined, TS) ->
				TS
			end,
		Cfg).

do_get_ts(Key, Cfg) ->
	do_get(Key, Cfg#db_config{flags = ['witness']}).


do_delete(Key, #db_config{table = Table, flags = Flags,
	timeout = Timeout} = Cfg) ->
	pass_hibari_request(
		{delete,
			Table,
			term_to_binary(Key),
			Flags, Timeout},
		fun () ->
			?R_OK end,
		Cfg).

%% -----------------------------------------------------------------------------
%% Hibari reply handling frunction
%% -----------------------------------------------------------------------------
pass_hibari_request(Req, Fun, Cfg) ->
	case do_hibari_request(Req, Cfg) of
		{reply, {ok, TS, Bin}, _} ->
			Fun(Bin, TS);
		{reply, {ok, TS}, _} ->
			Fun(undefined, TS);
		{reply, ok, none} ->
			Fun();
		{reply, List, none} when is_list(List) ->
			Fun(txn_answer, List);
		{reply, key_not_exist, none} ->
			?E_DB_KEY_NOT_EXIST;
		_Other ->
			?E_DB_ERROR
	end.

%% -----------------------------------------------------------------------------
%% UBF/TCP request
%% -----------------------------------------------------------------------------
do_hibari_request(Req, #db_config{host = Host, port = Port,
	timeout = Timeout}) ->
	case ubf_client:connect(Host, Port, [{proto, ubf}],
		Timeout) of
			{ok, Pid, _} ->
				ubf_client:rpc(Pid, {startSession,
					{'#S', "gdss"}, []}),
				Res = ubf_client:rpc(Pid, Req),
				ubf_client:stop(Pid),
				Res;
			{error, _Smth} ->
				{error, db_error}
	end.
