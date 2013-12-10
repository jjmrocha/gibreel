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
			store(Key, Value, Record, true),
			ok
	end.

-spec get(CacheName :: atom(), Key :: term()) -> {ok, Value :: term()} | {error, Reason}
	when Reason :: not_found | no_cache | error.
get(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> {error, no_cache};
		Record -> 
			get(Key, Record, true)
	end.	

-spec remove(CacheName :: atom(), Key :: term()) -> no_cache | ok.
remove(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			remove(Key, Record, true),
			ok
	end.

-spec touch(CacheName :: atom(), Key :: term()) -> ok.
touch(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			touch(Key, Record, true),
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
	Reply = get(Key, Record, false),
	From ! {cluster_msg, {value, Reply}},
	{noreply, State};

handle_info({cluster_msg, {store, Key, Value}}, State=#state{record=Record}) ->
	store(Key, Value, Record, false),
	{noreply, State};

handle_info({cluster_msg, {touch, Key}}, State=#state{record=Record}) ->
	touch(Key, Record, false),
	{noreply, State};

handle_info({cluster_msg, {remove, Key}}, State=#state{record=Record}) ->
	remove(Key, Record, false),
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

store(Key, Value, Record=#cache_record{name=CacheName, config=Config, memory=DB}, Notify) ->
	Timeout = case Config#cache_config.max_age of
		?NO_MAX_AGE -> 0;
		_ -> current_time() + Config#cache_config.max_age
	end,
	Timestamp = insert_index(Key, DB#cache_memory.index),
	case ets:insert_new(DB#cache_memory.table, {Key, Value, Timestamp, Timeout}) of
		true -> 
			update_counter(Record, 1),
			cluster_notify(Notify, {store, Key, Value}, CacheName, Config#cache_config.cluster_nodes);
		false -> 
			remove(Key, Record, false),
			case ets:insert_new(DB#cache_memory.table, {Key, Value, Timestamp, Timeout}) of
				true -> 
					update_counter(Record, 1),
					cluster_notify(Notify, {store, Key, Value}, CacheName, Config#cache_config.cluster_nodes);
				false -> ok
			end
	end.

insert_index(_Key, ?NO_INDEX_TABLE) -> timestamp();
insert_index(Key, Index) -> 
	Timestamp = timestamp(),
	case ets:insert_new(Index, {Timestamp, Key}) of
		true -> Timestamp;
		false -> insert_index(Key, Index)
	end.

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
				TableSize > MaxSize -> delete_older(Record);
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
				[{_Timestamp, Key}] -> remove(Key, Record, false)
			end
	end.

remove(Key, Record=#cache_record{name=CacheName, config=Config, memory=DB}, Notify) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> ok;
		[{_Key, _Value, Timestamp, _Timeout}] ->
			delete_index(Timestamp, DB),
			case ets:delete(DB#cache_memory.table, Key) of
				true -> update_counter(Record, -1);
				false -> ok
			end
	end,
	cluster_notify(Notify, {remove, Key}, CacheName, Config#cache_config.cluster_nodes).

delete_index(_Timestamp, #cache_memory{index=?NO_INDEX_TABLE}) -> ok;
delete_index(Timestamp, #cache_memory{index=Index}) ->
	ets:delete(Index, Timestamp).

cluster_notify(false, _Msg, _CacheName, _Nodes) -> 0;
cluster_notify(_Notify, _Msg, _CacheName, ?CLUSTER_NODES_LOCAL) -> 0;
cluster_notify(true, Msg, CacheName, ?CLUSTER_NODES_ALL) ->
	spawn(fun() -> 
		columbo:send_to_all(CacheName, {cluster_msg, Msg}) 
	end);
cluster_notify(true, Msg, CacheName, Nodes) ->
	spawn(fun() -> 
		columbo:send_to_nodes(CacheName, Nodes, {cluster_msg, Msg}) 
	end).

touch(Key, #cache_record{name=CacheName, config=Config, memory=DB}, Notify) ->
	case Config#cache_config.max_size of
		?NO_MAX_AGE -> ok;
		Expire -> 
			Timeout = current_time() + Expire,
			case ets:update_element(DB#cache_memory.table, Key, {4, Timeout}) of
				true -> cluster_notify(Notify, {touch, Key}, CacheName, Config#cache_config.cluster_nodes);
				false -> ok
			end
	end.

purge(#cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> ok;
purge(Record=#cache_record{memory=DB}) ->
	Fun = fun() ->
		Now = current_time(),
		Match = [{{'$1','$2', '$3', '$4'},[{'<', '$4', Now}],['$1']}],
		Keys = ets:select(DB#cache_memory.table, Match),
		lists:foreach(fun(Key) ->
			remove(Key, Record, false)
		end, Keys)
	end,
	spawn(Fun).

get(Key, Record=#cache_record{config=Config, memory=DB}, UseCluster) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> 
			case find_value(Key, Record, UseCluster) of
				{ok, Value} -> {ok, Value};
				Other -> Other
			end;
		[{_Key, Value, _Timestamp, Timeout}] -> 
			case Config#cache_config.max_age of
				?NO_MAX_AGE -> {ok, Value};
				_ -> 
					Now = current_time(),
					if 
						Now =< Timeout -> {ok, Value};
						true -> 
							case find_value(Key, Record, UseCluster) of
								{ok, Value} -> {ok, Value};
								Other -> 
									spawn(fun() -> 
										remove(Key, Record, false) 
									end),
									Other
							end
					end
			end
	end.

find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION}}, false) -> not_found;
find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=?CLUSTER_NODES_LOCAL}}, _UseCluster) -> not_found;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}, true) -> 
	case cluster_get(Key, Record#cache_record.name, Nodes) of
		{ok, Value} ->
			spawn(fun() -> 
				store(Key, Value, Record, false) 
			end),
			{ok, Value};
		not_found -> not_found
	end;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=Function}}, _UseCluster) ->
	try Function(Key) of
		not_found -> not_found;
		error -> error;
		Value -> 
			spawn(fun() -> 
				store(Key, Value, Record, false) 
			end),
			{ok, Value}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.	

cluster_get(Key, CacheName, Nodes) ->
	RequestsSent = cluster_notify(true, {get, Key, self()}, CacheName, Nodes),
	receive_values(RequestsSent, 0).

receive_values(Size, Size) -> not_found;
receive_values(Size, Count) ->
	receive
		{cluster_msg, {value, {ok, Value}}} -> {ok, Value};
		{cluster_msg, {value, _}} -> receive_values(Size, Count + 1)
	after ?CLUSTER_TIMEOUT -> not_found
	end.

sync(#cache_record{config=#cache_config{sync_mode=?LAZY_SYNC_MODE}}) -> ok;
sync(Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) ->
	Fun = fun() ->
			Count = cluster_notify(true, {sync, self()}, Record#cache_record.name, Nodes),
			receive_keys(Count, Record)
	end,
	spawn(Fun);
sync(_Record) -> ok.

receive_keys(0, _Record) -> ok;
receive_keys(_, Record) -> 
	receive
		{cluster_msg, {keys, List}} -> request_values(List, Record)
	end.	

request_values([], _Record) -> ok;
request_values([Key|T], Record) -> 
	case cluster_get(Key, Record#cache_record.name, Record#cache_record.config#cache_config.cluster_nodes) of
		{ok, Value} -> store(Key, Value, Record, false);
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
