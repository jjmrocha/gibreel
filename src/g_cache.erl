%%
%% Copyright 2013 Joaquim Rocha
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

-module(g_cache).

-behaviour(gen_server).

-include("gibreel.hlr").

-define(RECORD_COUNTER, gcache_record_counter).
-define(NO_INDEX_TABLE, none).
-define(NO_TASK, none).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, stop/1]).
-export([store/3, get/2, remove/2]).
-export([size/1]).
-export([purge/1]).

start_link(CacheConfig) when is_record(CacheConfig, cache_config)->
	gen_server:start_link(?MODULE, [CacheConfig], []).

stop(Server) when is_pid(Server) ->
	gen_server:cast(Server, {stop}).

store(Server, Key, Value) when is_pid(Server) ->
	gen_server:cast(Server, {store, Key, Value}).

get(Server, Key) when is_pid(Server) ->
	gen_server:call(Server, {get, Key}).

remove(Server, Key) when is_pid(Server) ->
	gen_server:cast(Server, {remove, Key}).

size(Server) when is_pid(Server) ->
	gen_server:call(Server, {size}).

purge(Server) when is_pid(Server) ->
	gen_server:cast(Server, {purge}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(db, {table, index}).
-record(state, {config, db, task}).

%% init
init([CacheConfig]) ->
	Table = create_table(CacheConfig),
	Index = create_index(CacheConfig),
	DB = #db{table=Table, index=Index},
	Task = start_timer(CacheConfig),
	error_logger:info_msg("Cache ~p created on [~p]...\n", [CacheConfig#cache_config.cache_name, self()]),
	{ok, #state{config=CacheConfig, db=DB, task=Task}}.

%% handle_call
handle_call({get, Key}, From, State=#state{config=CacheConfig, db=DB}) ->
	run_get(Key, DB, CacheConfig, From),
	{noreply, State};

handle_call({size}, From, State=#state{config=CacheConfig, db=DB}) ->
	run_size(DB, CacheConfig, From),
	{noreply, State}.

%% handle_cast
handle_cast({store, Key, Value}, State=#state{config=CacheConfig, db=DB}) ->
	run_store(Key, Value, DB, CacheConfig),
	{noreply, State};

handle_cast({remove, Key}, State=#state{config=CacheConfig, db=DB}) ->
	run_remove(Key, DB, CacheConfig),
	{noreply, State};

handle_cast({purge}, State=#state{config=CacheConfig, db=DB}) ->
	run_purge(DB, CacheConfig),
	{noreply, State};

handle_cast({stop}, State) ->
	{stop, normal, State}.

%% handle_info
handle_info({run_purge}, State=#state{config=CacheConfig, db=DB}) ->
	run_purge(DB, CacheConfig),
	{noreply, State}.

%% terminate
terminate(_Reason, State) ->
	drop(State#state.db),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_table(#cache_config{cache_name=CacheName}) ->
	TableName = get_table_name(CacheName),
	Table = ets:new(TableName, [set, public, {read_concurrency, true}]),
	ets:insert(Table, {?RECORD_COUNTER, 0}),
	Table.

create_index(#cache_config{max_size=?NO_MAX_SIZE, prune=?NO_PRUNE}) -> ?NO_INDEX_TABLE;
create_index(#cache_config{cache_name=CacheName}) -> 
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

start_timer(#cache_config{prune=?NO_PRUNE}) -> ?NO_TASK;
start_timer(#cache_config{prune=Interval}) ->
	TimerInterval = Interval * 1000,
	{ok, Timer} = timer:send_interval(TimerInterval, {run_purge}),
	Timer.

run_store(Key, Value, DB, CacheConfig) ->
	Fun = fun() ->
			insert(Key, Value, DB, CacheConfig)
	end,
	spawn(Fun).

insert(Key, Value, DB=#db{table=Table, index=Index}, CacheConfig=#cache_config{expire=Expire}) ->
	Timeout = case Expire of
		?NO_MAX_AGE -> 0;
		_ -> current_time() + Expire
	end,
	Timestamp = insert_index(Key, Index),
	case ets:insert(Table, {Key, Value, Timestamp, Timeout}) of
		true -> update_counter(DB, 1,  CacheConfig);
		false -> ok
	end.

insert_index(_Key, ?NO_INDEX_TABLE) -> 0;
insert_index(Key, Index) -> 
	Timestamp = timestamp(),
	case ets:insert_new(Index, {Timestamp, Key}) of
		true -> Timestamp;
		false -> insert_index(Key, Index)
	end.

update_counter(DB=#db{table=Table}, Inc, CacheConfig=#cache_config{max_size=MaxSize}) ->
	TableSize = ets:update_counter(Table, ?RECORD_COUNTER, Inc),
	case MaxSize of
		?NO_MAX_SIZE -> ok;
		_ ->
			if 
				TableSize > MaxSize -> delete_older(DB, CacheConfig);
				true -> ok
			end
	end.

delete_older(#db{index=?NO_INDEX_TABLE}, _CacheConfig) -> ok;
delete_older(DB=#db{index=Index}, CacheConfig) ->
	case ets:first(Index) of
		'$end_of_table' -> ok;
		Timestamp -> 
			case ets:lookup(Index, Timestamp) of
				[] -> ok;
				[{_Timestamp, Key}] -> delete(Key, DB, CacheConfig)
			end
	end.

run_get(Key, DB=#db{table=Table}, CacheConfig=#cache_config{expire=Expire}, From)->
	Fun = fun() ->
			Reply = case ets:lookup(Table, Key) of
				[] -> 
					case find_value(key, DB, CacheConfig) of
						{ok, Value} -> {ok, Value};
						Other -> Other
					end;
				[{_Key, Value, _Timestamp, Timeout}] -> 
					case Expire of
						?NO_MAX_AGE -> {ok, Value};
						_ -> 
							Now = time(),
							if 
								Now =< Timeout -> Value;
								true -> 
									case find_value(key, DB, CacheConfig) of
										{ok, Value} -> {ok, Value};
										Other -> 
											delete(Key, DB, CacheConfig),
											Other
									end
							end
					end
			end,
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_size(#db{table=Table}, _CacheConfig, From)->
	Fun = fun() ->
			Reply = case ets:lookup(Table, ?RECORD_COUNTER) of
				[] -> 0;
				[{_Key, Count}] -> Count
			end,
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

find_value(_Key, _DB, #cache_config{function=?NO_FUNCTION}) -> not_found;
find_value(Key, DB, CacheConfig=#cache_config{function=Function}) ->
	try Function(Key) of
		Result -> 
			insert(Key, Result, DB, CacheConfig),
			{ok, Result}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.	

run_remove(Key, DB, CacheConfig) ->
	Fun = fun() ->
			delete(Key, DB, CacheConfig)
	end,
	spawn(Fun).

run_purge(#db{index=?NO_INDEX_TABLE}, _CacheConfig) -> ok;
run_purge(DB=#db{index=Index}, CacheConfig) ->
	Fun = fun() ->
			Now = time(),
			Match = [{{'$1','$2'},[{'<', '$1', Now}],['$2']}],
			Keys = ets:select(Index, Match),
			delete_all(Keys, DB, CacheConfig)
	end,
	spawn(Fun).

delete_all([], _DB, _CacheConfig) -> ok;
delete_all([Key|T], DB, CacheConfig) ->
	delete(Key, DB, CacheConfig),
	delete_all(T, DB, CacheConfig).

delete(Key, DB=#db{table=Table}, CacheConfig) ->
	delete_index(Key, DB),
	case ets:delete(Table, Key) of
		true -> update_counter(DB, -1, CacheConfig);
		false -> ok
	end.

delete_index(_Key, #db{index=?NO_INDEX_TABLE}) -> ok;
delete_index(Key, #db{table=Table, index=Index}) ->
	case ets:lookup(Table, Key) of
		[] -> ok;
		[{_Key, _Value, Timestamp, _Timeout}] ->
			ets:delete(Index, Timestamp)
	end.

timestamp() -> os:timestamp().

current_time() ->
	{Mega, Sec, _} = os:timestamp(),
	(Mega * 1000000) + Sec.

drop(#db{table=Table, index=Index}) ->
	ets:delete(Table),
	case Index of
		none -> ok;
		_ -> ets:delete(Index)
	end.