%%% ----------------------------------------------------------------------------
%%%     @title          General DB reply record
%%%     @author         Ilya Ponetayev <Ilya.Ponetaev@kodep.ru>
%%%     @version        0.1
%%%     Part of celestial library
%%% ----------------------------------------------------------------------------

-record(db_reply, {
	type,		% Reply type: ok, error (atom)
	infocode,	% Infocode (atom, used when error has happened)
	value,		% Value or list of values (when txn with many gets)
	ts}).		% Reply TS

%% ------------ Some typical messages ------------------------------------------
%% Everything's all right
-define(R_DB_OK, #db_reply{type = ok}).

%% Error replies
-define(E_DB_GENERAL, #db_reply{type = error}).
-define(E_DB_ERROR, #db_reply{type = error, infocode = db_error}).
-define(E_DB_KEY_NOT_EXIST, #db_reply{type = error, infocode = key_not_exist}).
-define(E_DB_TXN_FAIL, #db_reply{type = error, infocode = txn_fail}).

