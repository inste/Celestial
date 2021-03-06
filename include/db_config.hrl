%%% ----------------------------------------------------------------------------
%%%     @title          DB runtime configuration
%%%     @author         Ilya Ponetayev <Ilya.Ponetaev@kodep.ru>
%%%     @version        0.1
%%%     Part of celestial library
%%% ----------------------------------------------------------------------------

-record(db_config, {
	host,		% Remote db host
	port,		% Port
	timeout,	% Timeout in ms
	flags,		% Flags
	exptime,	
	table		% KVDB table
	}).
