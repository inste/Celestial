

-record(db_reply, {
	type,
	infocode,
	value,
	ts}).


-define(R_OK, #db_reply{type = ok}).

-define(E_DB_GENERAL, #db_reply{type = error}).
-define(E_DB_ERROR, #db_reply{type = error, infocode = db_error}).
-define(E_DB_KEY_NOT_EXIST, #db_reply{type = error, infocode = key_not_exist}).
