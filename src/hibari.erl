%%% ----------------------------------------------------------------------------
%%%     @title          Hibari operations handling unit
%%%     @author         Ilya Ponetayev <Ilya.Ponetaev@kodep.ru>
%%%     @version        0.2
%%%     Part of celestial library
%%% ----------------------------------------------------------------------------
-module(hibari).

-vsn(0.2).

-export([make_add/2, make_add/3, make_add/4, make_add/5]).
-export([make_set/2, make_set/3, make_set/4, make_set/5]).
-export([make_get/2, make_get/1]).
-export([make_txn/2, make_txn/3, make_txn/4]).
-export([do_add/3, do_set/3, do_replace/3]).
-export([do_get/2, do_get_ts/2]).
-export([do_delete/2, do_txn/2]).

-include("defaults.hrl").
-include("db_reply.hrl").
-include("db_config.hrl").

%% ---------------- Metacommands for building transactions ---------------------

%% -----------------------------------------------------------------------------
%% Make add operation
%% -----------------------------------------------------------------------------
make_add(Key, Value, TS, Exptime, Flags) ->
	make_op(add, Key, Value, TS, Exptime, Flags).

make_add(Key, Value, TS, Flags) ->
	make_add(Key, Value, TS, ?HIBARI_DEFAULT_EXPTIME, Flags).

make_add(Key, Value, TS) ->
	make_add(Key, Value, TS, ?HIBARI_DEFAULT_FLAGS).

make_add(Key, Value) ->
	make_add(Key, Value, lib:make_ts()).


%% -----------------------------------------------------------------------------
%% Make set operation
%% -----------------------------------------------------------------------------
make_set(Key, Value, TS, Exptime, Flags) ->
	make_op(set, Key, Value, TS, Exptime, Flags).

make_set(Key, Value, TS, Flags) ->
	make_set(Key, Value, TS, ?HIBARI_DEFAULT_EXPTIME, Flags).

make_set(Key, Value, TS) ->
	make_set(Key, Value, TS, ?HIBARI_DEFAULT_FLAGS).

make_set(Key, Value) ->
	make_set(Key, Value, lib:make_ts()).

%% -----------------------------------------------------------------------------
%% Make get operation
%% -----------------------------------------------------------------------------
make_get(Key, Flags) ->
	{get, term_to_binary(Key), Flags}.

make_get(Key) ->
	make_get(Key, ?HIBARI_DEFAULT_FLAGS).

%% -----------------------------------------------------------------------------
%% Make transaction
%% -----------------------------------------------------------------------------
make_txn(Table, Ops, Timeout, Flags) when is_list(Ops) ->
	{do, Table, [txn | Ops], Flags, Timeout}.

make_txn(Table, Ops, Flags) ->
	make_txn(Table, Ops, ?HIBARI_DEFAULT_TIMEOUT, Flags).

make_txn(Table, Ops) ->
	make_txn(Table, Ops, ?HIBARI_DEFAULT_FLAGS).

make_op(Op, Key, Value, TS, Exptime, Flags) ->
	{Op, term_to_binary(Key), TS, term_to_binary(Value), Exptime, Flags}.


%% -----------------------------------------------------------------------------
%% Hibari commands
%% -----------------------------------------------------------------------------
do_set(Key, Value, Cfg) ->
	do_hibari_cmd(set, Key, Value, Cfg).

do_add(Key, Value, Cfg) ->
	do_hibari_cmd(add, Key, Value, Cfg).

do_replace(Key, Value, Cfg) ->
	do_hibari_cmd(replace, Key, Value, Cfg).

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
			?C_OK end,
		Cfg).

do_hibari_cmd(Cmd, Key, Value, #db_config{table = Table,
	timeout = Timeout, flags = Flags, exptime = Exptime} = Cfg) ->
	pass_hibari_request(
		{Cmd,
			Table,
			term_to_binary(Key),
			term_to_binary(Value),
			Exptime, Flags, Timeout},
		fun () ->
			?C_OK end,
		Cfg).


do_txn(OpList, #db_config{table = Table, flags = Flags,
	timeout = Timeout} = Cfg) ->
	pass_hibari_request(
		make_txn(Table, OpList, Timeout, Flags),
		fun 
			(txn_answer, List) ->
				handle_txn_results(List);
			(txn_fail, _) ->
				?CE_TXN_FAIL
		end,
		Cfg).

%% -----------------------------------------------------------------------------
%% Hibari reply handling function
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
		{reply, {txn_fail, List}, none} ->
			Fun(txn_fail, List);
		{reply, key_not_exist, none} ->
			?CE_KEY_NOT_EXIST;
		_Other ->
			?CE_DB_ERROR
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
				?CE_DB_ERROR
	end.

%% -----------------------------------------------------------------------------
%% Handling microtransaction result
%% -----------------------------------------------------------------------------
handle_txn_results(ResList) ->
	{Read, Errored} =
		lists:foldl(
			fun
				(ok, Acc) ->
					Acc;
				({ok, TS, BinKey} = A, {R, E}) ->
					{[{ok, TS, binary_to_term(BinKey)}
						| R], E};
				({ok, _TS} = A, {R, E}) ->
					{[A | R], E};
				(key_not_exist, {R, E}) ->
					{R, [key_not_exist, E]};
				(_Other, {R, E}) ->
					{R, [error | E]}
			end,
			{[], []},
			lists:reverse(ResList)),
	case {length(Errored), length(Read)} of
		{0, 0} ->
			#db_reply{type = ok, infocode = txn_res,
				value = undefined};
		{0, Y} when Y > 0 ->
			#db_reply{type = ok, infocode = txn_res,
				value = Read};
		{X, _} when (X =/= 0) ->
			?CE_TXN_FAIL
	end.
