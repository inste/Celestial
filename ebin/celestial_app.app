%%% ----------------------------------------------------------------------------
%%%	@title		Celestial library
%%%	@author		Ilya Ponetayev <Ilya.Ponetaev@kodep.ru>
%%%	@version	0.1
%%%	Part of Celestial library
%%% ----------------------------------------------------------------------------

{application, celestial_app,
	[{description, "KVDB Celestial library"},
		{vsn, 0.1},
		{modules, [celestial_app, hibari, lib]},
		{registered, []},
		{applications, [kernel,stdlib]},
		{mod, {celestial_app, []}}
	]
}.
