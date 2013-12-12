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

-include("gibreel.hrl").

-define(RECORD_COUNTER, gcache_record_counter).
-define(NO_TASK, none).
-define(NO_COLUMBO, none).
-define(CLUSTER_TIMEOUT, 1000).
-define(SYNC_TIMEOUT, 5000).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).
-export([store/3, get/2, remove/2, touch/2]).
-export([size/1, foldl/3]).

start_link(CacheName) ->
	gen_server:start_link(?MODULE, [CacheName], []).

-spec store(CacheName :: atom(), Key :: term(), Value :: term()) -> no_cache | ok.
store(CacheName, Key, Value) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			run_store(Key, Value, Record),
			ok
	end.

-spec get(CacheName :: atom(), Key :: term()) -> {ok, Value :: term()} | not_found | no_cache | error.
get(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			run_get(Key, Record)
	end.	

-spec remove(CacheName :: atom(), Key :: term()) -> no_cache | ok.
remove(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			run_remove(Key, Record),
			ok
	end.

-spec touch(CacheName :: atom(), Key :: term()) -> ok.
touch(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			run_touch(Key, Record),
			ok
	end.	

-spec size(CacheName :: atom()) -> no_cache | integer().
size(CacheName) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		#cache_record{memory=#cache_memory{table=Table}} -> 
			case ets:lookup(Table, ?RECORD_COUNTER) of
				[] -> 0;
				[{_Key, Count}] -> Count
			end
	end.	

-spec foldl(CacheName :: atom(), Fun, Acc :: term()) -> no_cache | term()
	when Fun :: fun(({Key :: term(), Value :: term()}, Acc :: term()) -> term()).
foldl(CacheName, Fun, Acc) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		#cache_record{memory=#cache_memory{table=Table}} -> 
			ets:foldl(fun({Key, Value, _, _}, FoldValue) -> Fun({Key, Value}, FoldValue);
					(_, FoldValue) -> FoldValue
				end, Acc, Table)
	end.	

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(state, {record, task}).

%% init
init([CacheName]) ->
	case get_cache_record(CacheName) of
		no_cache -> 
			error_logger:error_msg("Cache ~p not created, record not found!", [CacheName]),
			{stop, no_cache};
		Record=#cache_record{config=Config, memory=DB} ->
			erlang:register(CacheName, self()),
			NewDB = create_db(CacheName, Config, DB),
			NewRecord = Record#cache_record{memory=NewDB},
			ets:insert(?GIBREEL_TABLE, NewRecord),
			Task = start_timer(Config),
			setup_columbo(NewRecord),
			error_logger:info_msg("Cache ~p created on [~p]...\n", [CacheName, self()]),
			sync(NewRecord),
			{ok, #state{record=NewRecord, task=Task}}			
	end.

%% handle_call
handle_call(Msg, _From, State) ->
	error_logger:info_msg("handle_call(~p)", [Msg]),
	{noreply, State}.

%% handle_cast
handle_cast(Msg, State) ->
	error_logger:info_msg("handle_cast(~p)", [Msg]),
	{noreply, State}.

%% handle_info
handle_info({cluster_msg, {get, Key, From}}, State=#state{record=Record}) ->
	Reply = select(Key, Record),
	From ! {cluster_msg, {value, Reply}},
	{noreply, State};

handle_info({cluster_msg, {store, Key, Value, Timestamp}}, State=#state{record=Record}) ->
	run_store(Key, Value, Timestamp, Record),
	{noreply, State};

handle_info({cluster_msg, {touch, Key}}, State=#state{record=Record}) ->
	update_timeout(Key, Record),
	{noreply, State};

handle_info({cluster_msg, {remove, Key, Timestamp}}, State=#state{record=Record}) ->
	run_remove(Key, Timestamp, Record),
	{noreply, State};

handle_info({run_purge}, State=#state{record=Record}) ->
	purge(Record),
	{noreply, State};

handle_info({cluster_msg, {sync, From}}, State=#state{record=Record}) ->
	run_sync(Record, From),
	{noreply, State};

handle_info({stop_cache}, State) ->
	{stop, normal, State}.

%% terminate
terminate(_Reason, #state{record=#cache_record{name=CacheName, memory=DB}, task=Task}) ->
	stop_timer(Task),
	erlang:unregister(CacheName),
	drop(DB),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

get_cache_record(CacheName) ->
	case ets:lookup(?GIBREEL_TABLE, CacheName) of
		[Record] -> Record;
		_ -> no_cache
	end.

create_db(CacheName, Config, ?NO_DATA) ->
	Table = create_table(CacheName),
	Index = create_index(CacheName, Config),
	#cache_memory{table=Table, index=Index};
create_db(_CacheName, _Config, DB) -> DB.

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

start_timer(#cache_config{purge_interval=?NO_PURGE}) -> ?NO_TASK;
start_timer(#cache_config{purge_interval=Interval}) ->
	TimerInterval = Interval * 1000,
	{ok, Task} = timer:send_interval(TimerInterval, {run_purge}),
	Task.

setup_columbo(#cache_record{config=#cache_config{cluster_nodes=?CLUSTER_NODES_LOCAL}}) -> ok;
setup_columbo(#cache_record{config=#cache_config{cluster_nodes=?CLUSTER_NODES_ALL}}) -> ok;
setup_columbo(#cache_record{config=#cache_config{cluster_nodes=Nodes}}) -> 
	columbo:add_nodes(Nodes).

run_store(Key, Value, Record=#cache_record{name=CacheName, config=Config}) ->
	Timestamp = brute_force(fun(T) ->
					run_store(Key, Value, T, Record)
			end, timestamp()),
	spawn(fun() ->
			cluster_notify(CacheName, {store, Key, Value, Timestamp}, Config#cache_config.cluster_nodes)
		end).

brute_force(Fun, Timestamp) ->
	case Fun(Timestamp) of
		ok -> Timestamp;
		{false, StoredTimestamp} -> brute_force(Fun, StoredTimestamp + 1)
	end.

run_store(Key, Value, Timestamp, Record=#cache_record{config=Config, memory=DB}) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> 
			insert(Key, Value, Timestamp, get_timeout(Config), Record),
			ok;
		[{_Key, _Value, StoredTimestamp, _Timeout}] ->
			if StoredTimestamp > Timestamp -> {false, StoredTimestamp};
				true -> 
					delete(Key, StoredTimestamp, Record),
					insert(Key, Value, Timestamp, get_timeout(Config), Record),
					ok
			end
	end.

get_timeout(#cache_config{max_age=?NO_MAX_AGE}) -> 0;
get_timeout(#cache_config{max_age=Expire}) ->
	current_time() + Expire.

current_time() ->
	{Mega, Sec, _} = os:timestamp(),
	(Mega * 1000000) + Sec.

timestamp() ->
	cclock:cluster_timestamp().

update_counter(Record=#cache_record{config=Config, memory=DB}, Inc) ->
	TableSize = ets:update_counter(DB#cache_memory.table, ?RECORD_COUNTER, Inc),
	case Config#cache_config.max_size of
		?NO_MAX_SIZE -> ok;
		MaxSize ->
			if 
				TableSize > MaxSize ->
					spawn(fun() ->
								delete_older(Record)
						end);
				true -> ok
			end
	end.

delete_older(#cache_record{memory=#cache_memory{index=?NO_INDEX_TABLE}}) -> ok;
delete_older(Record=#cache_record{memory=DB}) ->
	case ets:first(DB#cache_memory.index) of
		'$end_of_table' -> ok;
		Timestamp -> 
			case ets:lookup(DB#cache_memory.index, Timestamp) of
				[] -> ok;
				[{_Timestamp, Key}] -> delete(Key, Timestamp, Record)
			end
	end.

run_remove(Key, Record=#cache_record{name=CacheName, config=Config}) ->
	Timestamp = brute_force(fun(T) ->
					run_remove(Key, T, Record)
			end, timestamp()),
	spawn(fun() ->
			cluster_notify(CacheName, {remove, Key, Timestamp}, Config#cache_config.cluster_nodes)
		end).

run_remove(Key, Timestamp, Record=#cache_record{memory=DB}) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> ok;
		[{_Key, _Value, StoredTimestamp, _Timeout}] ->
			if StoredTimestamp > Timestamp -> {false, StoredTimestamp};
				true -> 
					delete(Key, StoredTimestamp, Record),
					ok
			end
	end.

cluster_notify(_CacheName, _Msg, ?CLUSTER_NODES_LOCAL) -> 0;
cluster_notify(CacheName, Msg, ?CLUSTER_NODES_ALL) ->
	columbo:send_to_all(CacheName, {cluster_msg, Msg});
cluster_notify(CacheName, Msg, Nodes) ->
	columbo:send_to_nodes(CacheName, Nodes, {cluster_msg, Msg}).

run_touch(Key, Record=#cache_record{name=CacheName, config=Config}) ->
	case update_timeout(Key, Record) of
		true ->
			spawn(fun() ->
					cluster_notify(CacheName, {touch, Key}, Config#cache_config.cluster_nodes)
				end);
		false -> ok
	end.

update_timeout(_Key, #cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> false;
update_timeout(Key, #cache_record{config=Config, memory=DB}) ->
	Timeout = get_timeout(Config),
	ets:update_element(DB#cache_memory.table, Key, {4, Timeout}).

purge(#cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> ok;
purge(Record=#cache_record{memory=DB}) ->
	Fun = fun() ->
			Now = current_time(),
			Match = [{{'$1','$2','$3','$4'},[{'<','$4',Now}],[{{'$1','$3'}}]}],
			Keys = ets:select(DB#cache_memory.table, Match),
			lists:foreach(fun({Key, Timestamp}) ->
						delete(Key, Timestamp, Record)
				end, Keys)
	end,
	spawn(Fun).

run_get(Key, Record=#cache_record{config=Config, memory=DB}) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> 
			case find_value(Key, Record) of
				{ok, Value} -> {ok, Value};
				Other -> Other
			end;
		[{_Key, Value, Timestamp, Timeout}] -> 
			case Config#cache_config.max_age of
				?NO_MAX_AGE -> {ok, Value};
				_ -> 
					Now = current_time(),
					if Now =< Timeout -> {ok, Value};
						true -> 
							case find_value(Key, Record) of
								{ok, Value} -> {ok, Value};
								Other -> 
									spawn(fun() -> 
												delete(Key, Timestamp, Record) 
										end),
									Other
							end
					end
			end
	end.

find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=?CLUSTER_NODES_LOCAL}}) -> 
	not_found;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) -> 
	case cluster_get(Key, Record#cache_record.name, Nodes) of
		{ok, Value, Timestamp} ->
			spawn(fun() -> 
						run_store(Key, Value, Timestamp, Record) 
				end),
			{ok, Value};
		not_found -> not_found
	end;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=Function}}) ->
	try Function(Key) of
		not_found -> not_found;
		error -> error;
		Value -> 
			spawn(fun() -> 
						run_store(Key, Value, timestamp(), Record) 
				end),
			{ok, Value}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.	

cluster_get(Key, CacheName, Nodes) ->
	RequestsSent = cluster_notify(CacheName, {get, Key, self()}, Nodes),
	receive_values(RequestsSent, 0).

receive_values(Size, Size) -> not_found;
receive_values(Size, Count) ->
	receive
		{cluster_msg, {value, {ok, Value, Timestamp}}} -> {ok, Value, Timestamp};
		{cluster_msg, {value, _}} -> receive_values(Size, Count + 1)
	after ?CLUSTER_TIMEOUT -> not_found
	end.

sync(#cache_record{config=#cache_config{sync_mode=?LAZY_SYNC_MODE}}) -> ok;
sync(Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) ->
	Fun = fun() ->
			Count = cluster_notify(Record#cache_record.name, {sync, self()}, Nodes),
			KeyList = receive_keys(Count),
			request_values(KeyList, Record)
	end,
	spawn(Fun);
sync(_Record) -> ok.

receive_keys(0) -> ok;
receive_keys(_) -> 
	receive
		{cluster_msg, {keys, List}} -> List
	after ?SYNC_TIMEOUT -> []
	end.	

request_values([], _Record) -> ok;
request_values([Key|T], Record) -> 
	case cluster_get(Key, Record#cache_record.name, Record#cache_record.config#cache_config.cluster_nodes) of
		{ok, Value, Timestamp} -> run_store(Key, Value, Timestamp, Record);
		_ -> ok
	end,
	request_values(T, Record).

run_sync(#cache_record{memory=#cache_memory{table=Table}}, From) ->
	Fun = fun() ->
			Keys = ets:select(Table, [{{'$1','$2','$3','$4'},[],['$1']}]),
			From ! {cluster_msg, {keys, Keys}}
	end,
	spawn(Fun).

drop(#cache_memory{table=Table, index=Index}) ->
	ets:delete(Table),
	case Index of
		none -> ok;
		_ -> ets:delete(Index)
	end.

stop_timer(?NO_TASK) -> ok;
stop_timer(Task) ->
	timer:cancel(Task).

select(Key, #cache_record{memory=#cache_memory{table=Table}}) ->
	case ets:lookup(Table, Key) of
		[] -> not_found;
		[{_Key, Value, Timestamp, _Timeout}] -> {ok, Value, Timestamp}
	end.	

insert(Key, Value, Timestamp, Timeout, Record=#cache_record{memory=#cache_memory{table=Table, index=Index}}) ->
	insert_index(Key, Timestamp, Index),
	ets:insert(Table, {Key, Value, Timestamp, Timeout}),
	update_counter(Record, 1).

insert_index(_Key, _Timestamp, ?NO_INDEX_TABLE) -> ok;
insert_index(Key, Timestamp, Index) -> 
	ets:insert(Index, {Timestamp, Key}).

delete(Key, Timestamp, Record=#cache_record{memory=#cache_memory{table=Table, index=Index}}) ->
	delete_index(Timestamp, Index),
	ets:delete(Table, Key),
	update_counter(Record, -1).

delete_index(_Timestamp, ?NO_INDEX_TABLE) -> ok;
delete_index(Timestamp, Index) ->
	ets:delete(Index, Timestamp).