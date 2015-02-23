%%
%% Copyright 2013-14 Joaquim Rocha
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(gibreel_db).

-include("gibreel_db.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([create/0, drop/0]).
-export([find/1, store/1, delete/1]).
-export([list_caches/0]).

create() ->
	Options = [set, public, named_table, {keypos, 2}, {read_concurrency, true}],
	ets:new(?GIBREEL_TABLE, Options).

drop() ->
	ets:delete(?GIBREEL_TABLE).

find(CacheName) ->
	case ets:lookup(?GIBREEL_TABLE, CacheName) of
		[#cache_record{config=Config}] -> {ok, Config};
		_ -> {error, no_cache}
	end.

store(Record) ->
	ets:insert(?GIBREEL_TABLE, Record).

delete(CacheName) ->
	ets:delete(?GIBREEL_TABLE, CacheName).

list_caches() ->
	ets:foldl(fun(#cache_record{name=Cache}, Acc) -> 
				[Cache|Acc] 
		end, [], ?GIBREEL_TABLE).

%% ====================================================================
%% Internal functions
%% ====================================================================


