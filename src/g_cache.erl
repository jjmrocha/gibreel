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

-module(g_cache).

-behaviour(gen_server).

-include("gibreel.hrl").

-type options() :: [{atom(), any()}, ...].
-export_type([options/0]).

-define(RECORD_COUNTER, gcache_record_counter).
-define(NO_TASK, none).
-define(NO_COLUMBO, none).
-define(CLUSTER_TIMEOUT, 1000).
-define(SYNC_TIMEOUT, 5000).

-define(NO_VERSION, no_vs).
-define(NO_TOUCH, no_touch).
-define(DEFAULT_VALUE, default).
-define(NO_RECORD, no_record).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).
-export([store/3, store/4]).
-export([get/2, getv/2]).
-export([gat/2, gat/3, gatv/2, gatv/3]).
-export([remove/2, remove/3]).
-export([touch/2, touch/3]).
-export([size/1, foldl/3, flush/1]).

start_link(CacheName) ->
	gen_server:start_link(?MODULE, [CacheName], []).

-spec store(CacheName :: atom(), Key :: term(), Value :: term()) -> no_cache | {ok, Version :: integer()}.
store(CacheName, Key, Value) ->
	store(CacheName, Key, Value, []).

-spec store(CacheName :: atom(), Key :: term(), Value :: term(), Options :: options()) -> no_cache | invalid_version | {ok, Version :: integer()}.
store(CacheName, Key, Value, Options) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record ->
			Version = get_option_value(?OPTION_VERSION, Options, ?NO_VERSION),
			Delay = get_option_value(?OPTION_DELAY, Options, ?DEFAULT_VALUE),
			run_store(Key, Value, Version, Delay, Record)
	end.

-spec getv(CacheName :: atom(), Key :: term()) -> {ok, Value :: term(), Version :: integer()} | not_found | no_cache | error.
getv(CacheName, Key) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			get_and_touch(Key, ?NO_TOUCH, Record)
	end.	

-spec get(CacheName :: atom(), Key :: term()) -> {ok, Value :: term()} | not_found | no_cache | error.
get(CacheName, Key) ->
	case getv(CacheName, Key) of
		{ok, Value, _Version} -> {ok, Value};
		Other -> Other
	end.

-spec remove(CacheName :: atom(), Key :: term()) -> no_cache | ok.
remove(CacheName, Key) ->
	remove(CacheName, Key, []).

-spec remove(CacheName :: atom(), Key :: term(), Options :: options()) -> no_cache | invalid_version | ok.
remove(CacheName, Key, Options) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record ->
			Version = get_option_value(?OPTION_VERSION, Options, ?NO_VERSION),
			run_remove(Key, Version, Record)
	end.

-spec touch(CacheName :: atom(), Key :: term()) -> no_cache | ok.
touch(CacheName, Key) ->
	touch(CacheName, Key, []).	

-spec touch(CacheName :: atom(), Key :: term(), Options :: options()) -> no_cache | ok.
touch(CacheName, Key, Options) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			Delay = get_option_value(?OPTION_DELAY, Options, ?DEFAULT_VALUE),
			run_touch(Key, Delay, Record),
			ok
	end.	

-spec gatv(CacheName :: atom(), Key :: term(), Options :: options()) -> {ok, Value :: term(), Version :: integer()} | not_found | no_cache | error.
gatv(CacheName, Key, Options) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			Delay = get_option_value(?OPTION_DELAY, Options, ?DEFAULT_VALUE),
			get_and_touch(Key, Delay, Record)
	end.

-spec gatv(CacheName :: atom(), Key :: term()) -> {ok, Value :: term(), Version :: integer()} | not_found | no_cache | error.
gatv(CacheName, Key) ->
	gatv(CacheName, Key, []).	

-spec gat(CacheName :: atom(), Key :: term(), Options :: options()) -> {ok, Value :: term()} | not_found | no_cache | error.
gat(CacheName, Key, Options) ->
	case gatv(CacheName, Key, Options) of
		{ok, Value, _Version} -> {ok, Value};
		Other -> Other
	end.

-spec gat(CacheName :: atom(), Key :: term()) -> {ok, Value :: term()} | not_found | no_cache | error.
gat(CacheName, Key) ->
	gat(CacheName, Key, []).

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

-spec flush(CacheName :: atom()) -> no_cache | ok.
flush(CacheName) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		Record -> 
			Config = Record#cache_record.config,
			Timestamp = timestamp(),
			cluster_notify(CacheName, {flush, Timestamp}, Config#cache_config.cluster_nodes),
			run_flush(Timestamp, Record),
			ok
	end.		

-spec foldl(CacheName :: atom(), Fun, Acc :: term()) -> no_cache | term()
	when Fun :: fun(({Key :: term(), Value :: term()}, Acc :: term()) -> term()).
foldl(CacheName, Fun, Acc) ->
	case get_cache_record(CacheName) of
		no_cache -> no_cache;
		#cache_record{memory=#cache_memory{table=Table}} -> 
			ets:foldl(fun({Key, Value, Version, _}, FoldValue) -> Fun(Key, Value, Version, FoldValue);
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

handle_info({cluster_msg, {store, Key, Value, Timestamp, Delay}}, State=#state{record=Record}) ->
	run_store(Key, Value, ?NO_VERSION, Delay, Timestamp, Record),
	{noreply, State};

handle_info({cluster_msg, {touch, Key, Delay}}, State=#state{record=Record}) ->
	update_timeout(Key, Delay, Record),
	{noreply, State};

handle_info({cluster_msg, {remove, Key, Timestamp}}, State=#state{record=Record}) ->
	run_remove(Key, ?NO_VERSION, Timestamp, Record),
	{noreply, State};

handle_info({run_purge}, State=#state{record=Record}) ->
	purge(Record),
	{noreply, State};

handle_info({cluster_msg, {sync, From}}, State=#state{record=Record}) ->
	run_sync(Record, From),
	{noreply, State};

handle_info({cluster_msg, {flush, Timestamp}}, State=#state{record=Record}) ->
	run_flush(Record, Timestamp),
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

% API

run_store(Key, Value, Version, Delay, Record=#cache_record{name=CacheName, config=Config}) ->
	Fun = fun(T) ->
			run_store(Key, Value, Version, Delay, T, Record)
	end,
	case brute_force(Fun, timestamp()) of
		{ok, Timestamp} ->
			spawn(fun() ->
						cluster_notify(CacheName, {store, Key, Value, Timestamp, Delay}, Config#cache_config.cluster_nodes)
				end),
			{ok, Timestamp};
		invalid_version -> invalid_version
	end.

get_and_touch(Key, Delay, Record=#cache_record{config=Config, memory=DB}) ->
	case read(DB, Key) of
		{not_found} ->
			case find_value(Key, Record) of
				{ok, Value, Version} -> {ok, Value, Version};
				Other -> Other
			end;
		{found, Value, Version, Timeout} ->
			case Config#cache_config.max_age of
				?NO_MAX_AGE ->
					touch_if_needed(Key, Delay, Record),
					{ok, Value, Version};
				_ -> 
					Now = current_time(),
					if Now =< Timeout ->
							touch_if_needed(Key, Delay, Record),
							{ok, Value, Version};
						true -> 
							case find_value(Key, Record) of
								{ok, Value, Version} -> {ok, Value, Version};
								Other -> 
									spawn(fun() -> 
												delete(Key, Version, Record) 
										end),
									Other
							end
					end
			end			
	end.

run_remove(Key, Version, Record=#cache_record{name=CacheName, config=Config}) ->
	Fun = fun(T) ->
			run_remove(Key, Version, T, Record)
	end,
	case brute_force(Fun, timestamp()) of
		{ok, Timestamp} ->
			spawn(fun() ->
						cluster_notify(CacheName, {remove, Key, Timestamp}, Config#cache_config.cluster_nodes)
				end),
			ok;
		invalid_version -> invalid_version
	end.

run_touch(Key, Delay, Record=#cache_record{name=CacheName, config=Config}) ->
	case update_timeout(Key, Delay, Record) of
		true ->
			spawn(fun() ->
						cluster_notify(CacheName, {touch, Key, Delay}, Config#cache_config.cluster_nodes)
				end);
		false -> ok
	end.

run_flush(RefDate, Record=#cache_record{memory=#cache_memory{table=Table}}) ->
	Fun = fun() ->
			Match = [{{'$1','$2','$3','$4'},[{'<','$3',RefDate}],[{{'$1','$3'}}]}],
			Keys = ets:select(Table, Match),
			lists:foreach(fun({Key, Timestamp}) ->
						delete(Key, Timestamp, Record)
				end, Keys)
	end,
	spawn(Fun).

% Server and API

sync(#cache_record{config=#cache_config{sync_mode=?LAZY_SYNC_MODE}}) -> ok;
sync(Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) ->
	Fun = fun() ->
			Count = cluster_notify(Record#cache_record.name, {sync, self()}, Nodes),
			KeyList = receive_keys(Count),
			request_values(KeyList, Record)
	end,
	spawn(Fun);
sync(_Record) -> ok.

run_store(Key, Value, Version, Delay, Timestamp, Record=#cache_record{config=Config, memory=DB}) ->
	case validate_change(DB, Key, Version, Timestamp) of
		insert -> 
			insert(Key, Value, Timestamp, get_timeout(Delay, Config), Record),
			ok;
		{update, StoredTimestamp} -> 
			delete(Key, StoredTimestamp, Record),
			insert(Key, Value, Timestamp, get_timeout(Delay, Config), Record),
			ok;
		Other -> Other
	end.

find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=?CLUSTER_NODES_LOCAL}}) -> 
	not_found;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) -> 
	case cluster_get(Key, Record#cache_record.name, Nodes) of
		{ok, Value, Timestamp} ->
			spawn(fun() -> 
						run_store(Key, Value, ?NO_VERSION, Timestamp, Record) 
				end),
			{ok, Value, Timestamp};
		not_found -> not_found
	end;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=Function}}) ->
	try Function(Key) of
		not_found -> not_found;
		error -> error;
		Value -> 
			Timestamp = timestamp(),
			spawn(fun() -> 
						run_store(Key, Value, ?NO_VERSION, Timestamp, Record) 
				end),
			{ok, Value, Timestamp}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.

touch_if_needed(_Key, ?NO_TOUCH, _Record) -> ok;
touch_if_needed(Key, Delay, Record) -> 
	run_touch(Key, Delay, Record).

run_remove(Key, Version, Timestamp, Record=#cache_record{memory=DB}) ->
	case validate_change(DB, Key, Version, Timestamp) of
		insert -> ok;
		{update, StoredTimestamp} -> 
			delete(Key, StoredTimestamp, Record),
			ok;
		Other -> Other
	end.

update_timeout(_Key, _Delay, #cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> false;
update_timeout(Key, Delay, #cache_record{config=Config, memory=DB}) ->
	Timeout = get_timeout(Delay, Config),
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

select(Key, #cache_record{memory=DB}) ->
	case read(DB, Key) of
		{not_found} -> not_found;
		{found, Value, StoredTimestamp, _Timeout} -> {ok, Value, StoredTimestamp}
	end.	

run_sync(#cache_record{memory=#cache_memory{table=Table}}, From) ->
	Fun = fun() ->
			Keys = ets:select(Table, [{{'$1','$2','$3','$4'},[],['$1']}]),
			From ! {cluster_msg, {keys, Keys}}
	end,
	spawn(Fun).

% BL Utils

receive_keys(0) -> [];
receive_keys(_) -> 
	receive
		{cluster_msg, {keys, List}} -> List
	after ?SYNC_TIMEOUT -> []
	end.	

request_values([], _Record) -> ok;
request_values([Key|T], Record) -> 
	case cluster_get(Key, Record#cache_record.name, Record#cache_record.config#cache_config.cluster_nodes) of
		{ok, Value, Timestamp} -> run_store(Key, Value, ?NO_VERSION, ?DEFAULT_VALUE, Timestamp, Record);
		_ -> ok
	end,
	request_values(T, Record).

validate_change(DB, Key, Version, Timestamp) ->
	case read(DB, Key) of
		{not_found} -> validate_operation(Version, ?NO_RECORD, Timestamp);
		{found, _Value, StoredTimestamp, _Timeout} -> validate_operation(Version, StoredTimestamp, Timestamp)
	end.

validate_operation(?NO_VERSION, ?NO_RECORD, _Timestamp) -> insert;
validate_operation(?NO_VERSION, StoredTimestamp, Timestamp) ->
	case StoredTimestamp =< Timestamp of
		true -> {update, StoredTimestamp};
		_ -> {false, StoredTimestamp}
	end;
validate_operation(_Version, ?NO_RECORD, _Timestamp) -> {false, invalid_version};
validate_operation(StoredTimestamp, StoredTimestamp, Timestamp) ->
	case StoredTimestamp =< Timestamp of
		true -> {update, StoredTimestamp};
		_ -> {false, StoredTimestamp}
	end;
validate_operation(_Version, _StoredTimestamp, _Timestamp) -> {false, invalid_version}.

get_timeout(_Delay, #cache_config{max_age=?NO_MAX_AGE}) -> 0;
get_timeout(?DEFAULT_VALUE, #cache_config{max_age=Expire}) ->
	current_time() + Expire;
get_timeout(Delay, _Config) ->
	current_time() + Delay.

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

% Low level

read(DB, Key) ->
	case ets:lookup(DB#cache_memory.table, Key) of
		[] -> {not_found};
		[{_Key, Value, StoredTimestamp, Timeout}] -> {found, Value, StoredTimestamp, Timeout}
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

% System

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

cluster_notify(_CacheName, _Msg, ?CLUSTER_NODES_LOCAL) -> 0;
cluster_notify(CacheName, Msg, ?CLUSTER_NODES_ALL) ->
	columbo:send_to_all(CacheName, {cluster_msg, Msg});
cluster_notify(CacheName, Msg, Nodes) ->
	columbo:send_to_nodes(CacheName, Nodes, {cluster_msg, Msg}).

drop(#cache_memory{table=Table, index=Index}) ->
	ets:delete(Table),
	case Index of
		none -> ok;
		_ -> ets:delete(Index)
	end.

stop_timer(?NO_TASK) -> ok;
stop_timer(Task) ->
	timer:cancel(Task).

% Util

brute_force(Fun, Timestamp) ->
	case Fun(Timestamp) of
		ok -> {ok, Timestamp};
		{false, invalid_version} -> invalid_version;
		{false, StoredTimestamp} -> brute_force(Fun, StoredTimestamp + 1)
	end.

timestamp() ->
	cclock:cluster_timestamp().

current_time() ->
	{Mega, Sec, _} = os:timestamp(),
	(Mega * 1000000) + Sec.

get_option_value(Tag, Options, Default) ->
	case lists:keyfind(Tag, 1, Options) of
		false -> Default;
		{_, Value} -> Value
	end.