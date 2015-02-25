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

-module(g_storage).

-include("gibreel.hrl").
-include("gibreel_db.hrl").
-include("g_storage.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([create/3, drop/1]).
-export([insert/5, delete/3, touch/3]).
-export([find/2, keys/1, keys/2]).
-export([size/1, foldl/3]).
-export([expired/2, older/1]).

create(CacheName, Config, ?NO_STORAGE) ->
	Table = create_table(CacheName),
	Index = create_index(CacheName, Config),
	#cache_storage{table=Table, index=Index};
create(_CacheName, _Config, DB) -> DB.

drop(#cache_storage{table=Table, index=Index}) ->
	ets:delete(Table),
	case Index of
		none -> ok;
		_ -> ets:delete(Index)
	end.

insert(#cache_storage{table=Table, index=Index}, Key, Value, Version, Timeout) ->
	insert_index(Key, Version, Index),
	ets:insert(Table, ?DATA_RECORD(Key, Value, Version, Timeout)),
	ets:update_counter(Table, ?RECORD_COUNTER, 1).

delete(#cache_storage{table=Table, index=Index}, Key, Version) ->
	delete_index(Version, Index),
	ets:delete(Table, Key),
	ets:update_counter(Table, ?RECORD_COUNTER, -1).

touch(#cache_storage{table=Table}, Key, Timeout) ->
	ets:update_element(Table, Key, {4, Timeout}).

find(DB, Key) ->
	case ets:lookup(DB#cache_storage.table, Key) of
		[] -> {error, not_found};
		[?DATA_RECORD(_Key, Value, Version, Timeout)] -> {ok, Value, Version, Timeout}
	end.

keys(#cache_storage{table=Table}) ->
	Match = [{{'$1','$2','$3','$4'},[],['$1']}],
	ets:select(Table, Match).

keys(#cache_storage{table=Table}, OlderThan) ->
	Match = [{{'$1','$2','$3','$4'},[{'<','$3',OlderThan}],[{{'$1','$3'}}]}],
	ets:select(Table, Match).

size(#cache_storage{table=Table}) -> 
	case ets:lookup(Table, ?RECORD_COUNTER) of
		[] -> 0;
		[{_Key, Count}] -> Count
	end.

foldl(#cache_storage{table=Table}, Fun, Acc) when is_function(Fun, 4) -> 
	ets:foldl(fun(?DATA_RECORD(Key, Value, Version, _), FoldValue) -> Fun(Key, Value, Version, FoldValue);
			(_, FoldValue) -> FoldValue
		end, Acc, Table);
foldl(#cache_storage{table=Table}, Fun, Acc) -> 
	ets:foldl(fun(?DATA_RECORD(Key, Value, _, _), FoldValue) -> Fun(Key, Value, FoldValue);
			(_, FoldValue) -> FoldValue
		end, Acc, Table).

expired(DB, When) ->
	Match = [{{'$1','$2','$3','$4'},[{'<','$4',When}],[{{'$1','$3'}}]}],
	ets:select(DB#cache_storage.table, Match).

older(#cache_storage{index=?NO_INDEX_TABLE}) -> {error, not_found};
older(#cache_storage{index=Index}) ->
	case ets:first(Index) of
		'$end_of_table' -> ok;
		Version -> 
			case ets:lookup(Index, Version) of
				[] -> {error, not_found};
				[?INDEX_RECORD(_Version, Key)] -> {ok, Key, Version}
			end
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_table(CacheName) ->
	TableName = get_table_name(CacheName),
	Table = ets:new(TableName, [set, public, {read_concurrency, true}]),
	ets:insert(Table, {?RECORD_COUNTER, 0}),
	Table.

create_index(_CacheName, #cache_config{max_size=?NO_MAX_SIZE}) -> ?NO_INDEX_TABLE;
create_index(CacheName, _Config) -> 
	IndexName = get_index_name(CacheName),
	ets:new(IndexName, [ordered_set, public]).

get_table_name(CacheName) ->
	make_name(CacheName, "_gcache_table").

get_index_name(CacheName) ->
	make_name(CacheName, "_gcache_index").

make_name(Name, Posfix) ->
	LName = atom_to_list(Name),
	ConcatName = LName ++ Posfix,
	list_to_atom(ConcatName).

insert_index(_Key, _Version, ?NO_INDEX_TABLE) -> ok;
insert_index(Key, Version, Index) -> 
	ets:insert(Index, ?INDEX_RECORD(Version, Key)).

delete_index(_Version, ?NO_INDEX_TABLE) -> ok;
delete_index(Version, Index) ->
	ets:delete(Index, Version).