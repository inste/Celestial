-module(lib).

-compile(export_all).


make_ts() ->
	{M, S, MS} = now(),
	(M * 1000000 + S) * 1000000 + MS.
