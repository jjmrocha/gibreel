%%
%% Copyright 2013-16 Joaquim Rocha
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
-include("gibreel_db.hrl").
-include("g_storage.hrl").

-type options() :: [{atom(), any()}, ...].
-export_type([options/0]).

-define(NO_TASK, none).
-define(NO_COLUMBO, none).
-define(CLUSTER_TIMEOUT, 1000).
-define(SYNC_TIMEOUT, 5000).

-define(NO_VERSION, no_vs).
-define(NO_TOUCH, no_touch).
-define(USE_DEFAULT_EXPIRE, default_expire).
-define(NO_RECORD, no_record).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).
-export([get/2, get/3]).
-export([store/3, store/4]).
-export([remove/2, remove/3]).
-export([touch/2, touch/3]).
-export([size/1, get_all_keys/1, foldl/3, flush/1]).
-export([reload/1]).

start_link(CacheName) ->
	gen_server:start_link(?MODULE, [CacheName], []).

-spec get(CacheName :: atom(), Key :: term()) -> {ok, Value :: term(), Version :: integer()} | not_found | no_cache | error.
get(CacheName, Key) ->
	get(CacheName, Key, []).

-spec get(CacheName :: atom(), Key :: term(), Options :: options()) -> {ok, Value :: term(), Version :: integer()} | not_found | no_cache | error.
get(CacheName, Key, Options) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> 
			Delay = get_option_value(?OPTION_DELAY, Options, ?NO_TOUCH),
			run_get(Key, Delay, Record)
	end.	

-spec store(CacheName :: atom(), Key :: term(), Value :: term()) -> no_cache | {ok, Version :: integer()}.
store(CacheName, Key, Value) ->
	store(CacheName, Key, Value, []).

-spec store(CacheName :: atom(), Key :: term(), Value :: term(), Options :: options()) -> no_cache | invalid_version | {ok, Version :: integer()}.
store(CacheName, Key, Value, Options) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> 
			OldVersion = get_option_value(?OPTION_VERSION, Options, ?NO_VERSION),
			Delay = get_option_value(?OPTION_DELAY, Options, ?USE_DEFAULT_EXPIRE),
			run_store(Key, Value, OldVersion, Delay, Record)
	end.

-spec remove(CacheName :: atom(), Key :: term()) -> no_cache | ok.
remove(CacheName, Key) ->
	remove(CacheName, Key, []).

-spec remove(CacheName :: atom(), Key :: term(), Options :: options()) -> no_cache | invalid_version | ok.
remove(CacheName, Key, Options) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> 
			Version = get_option_value(?OPTION_VERSION, Options, ?NO_VERSION),
			run_remove(Key, Version, Record)
	end.

-spec touch(CacheName :: atom(), Key :: term()) -> no_cache | ok.
touch(CacheName, Key) ->
	touch(CacheName, Key, []).	

-spec touch(CacheName :: atom(), Key :: term(), Options :: options()) -> no_cache | ok.
touch(CacheName, Key, Options) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> 
			Delay = get_option_value(?OPTION_DELAY, Options, ?USE_DEFAULT_EXPIRE),
			run_touch(Key, Delay, Record),
			ok
	end.	

-spec size(CacheName :: atom()) -> no_cache | integer().
size(CacheName) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> run_size(Record)
	end.	

-spec get_all_keys(CacheName :: atom()) -> no_cache | list().
get_all_keys(CacheName) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> run_get_keys(Record)
	end.	

-spec flush(CacheName :: atom()) -> no_cache | ok.
flush(CacheName) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> run_flush(Record)
	end.		

-spec foldl(CacheName :: atom(), Fun, Acc :: term()) -> no_cache | term()
	when Fun :: fun(({Key :: term(), Value :: term()}, Acc :: term()) -> term()).
foldl(CacheName, Fun, Acc) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> no_cache;
		{ok, Record} -> run_foldl(Fun, Acc, Record)
	end.	

-spec reload(CacheName :: atom()) -> ok.
reload(CacheName) ->
	gen_server:cast(CacheName, {reload_config}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(state, {record, task}).

%% init
init([CacheName]) ->
	case gibreel_db:find(CacheName) of
		{error, no_cache} -> 
			error_logger:error_msg("Cache ~p not created, record not found!", [CacheName]),
			{stop, no_cache};
		{ok, Record=#cache_record{config=Config, storage=DB}} ->
			erlang:register(CacheName, self()),
			NewDB = g_storage:create(CacheName, Config, DB),
			NewRecord = Record#cache_record{storage=NewDB},
			gibreel_db:store(NewRecord),
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
handle_cast({reload_config}, State=#state{record=#cache_record{name=CacheName}}) ->
	{ok, Record} = gibreel_db:find(CacheName),
	{noreply, State#state{record=Record}};

handle_cast(Msg, State) ->
	error_logger:info_msg("handle_cast(~p)", [Msg]),
	{noreply, State}.

%% handle_info
handle_info({cluster_msg, {get, Key, From, Ref}}, State=#state{record=Record}) ->
	async:run(fun() ->
				Reply = select(Key, Record),
				From ! {cluster_msg, {value, Ref, Reply}}
		end),
	{noreply, State};

handle_info({cluster_msg, {store, Key, Value, NewVersion, Delay}}, State=#state{record=Record}) ->
	async:run(fun() ->
				api_store(Key, Value, ?NO_VERSION, Delay, NewVersion, Record)
		end),
	{noreply, State};

handle_info({cluster_msg, {touch, Key, Delay}}, State=#state{record=Record}) ->
	async:run(fun() ->
				api_touch(Key, Delay, Record)
		end),
	{noreply, State};

handle_info({cluster_msg, {remove, Key, NewVersion}}, State=#state{record=Record}) ->
	async:run(fun() ->
				api_remove(Key, ?NO_VERSION, NewVersion, Record)
		end),	
	{noreply, State};

handle_info({run_purge}, State=#state{record=Record}) ->
	purge(Record),
	{noreply, State};

handle_info({cluster_msg, {sync, From}}, State=#state{record=Record}) ->
	run_sync(Record, From),
	{noreply, State};

handle_info({cluster_msg, {flush, Version}}, State=#state{record=Record}) ->
	api_flush(Version, Record),
	{noreply, State};

handle_info({stop_cache}, State) ->
	{stop, normal, State}.

%% terminate
terminate(_Reason, #state{record=#cache_record{name=CacheName, storage=DB}, task=Task}) ->
	stop_timer(Task),
	erlang:unregister(CacheName),
	g_storage:drop(DB),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

% API

run_get(Key, Delay, Record=#cache_record{config=Config, storage=DB}) ->
	case g_storage:find(DB, Key) of
		{error, not_found} ->
			case find_value(Key, Record) of
				{ok, Value, Version} -> {ok, Value, Version};
				Other -> Other
			end;
		{ok, Value, StoredVersion, Timeout} ->
			case Config#cache_config.max_age of
				?NO_MAX_AGE ->
					touch_if_needed(Key, Delay, Record),
					{ok, Value, StoredVersion};
				_ -> 
					Now = current_time(),
					if Now =< Timeout ->
							touch_if_needed(Key, Delay, Record),
							{ok, Value, StoredVersion};
						true -> 
							case find_value(Key, Record) of
								{ok, Value, Version} -> {ok, Value, Version};
								Other -> 
									async:run(fun() -> 
												delete(Key, StoredVersion, Record) 
										end),
									Other
							end
					end
			end			
	end.

run_store(Key, Value, OldVersion, Delay, Record=#cache_record{name=CacheName, config=Config}) ->
	Fun = fun(Version) ->
			api_store(Key, Value, OldVersion, Delay, Version, Record)
	end,
	case brute_force(Fun, version()) of
		{ok, Version} ->
			async:run(fun() ->
						cluster_notify(CacheName, {store, Key, Value, Version, Delay}, Config#cache_config.cluster_nodes)
				end),
			{ok, Version};
		invalid_version -> invalid_version
	end.

run_remove(Key, OldVersion, Record=#cache_record{name=CacheName, config=Config}) ->
	Fun = fun(Version) ->
			api_remove(Key, OldVersion, Version, Record)
	end,
	case brute_force(Fun, version()) of
		{ok, Version} ->
			async:run(fun() ->
						cluster_notify(CacheName, {remove, Key, Version}, Config#cache_config.cluster_nodes)
				end),
			ok;
		invalid_version -> invalid_version
	end.

run_touch(Key, Delay, Record=#cache_record{name=CacheName, config=Config}) ->
	case api_touch(Key, Delay, Record) of
		true ->
			async:run(fun() ->
						cluster_notify(CacheName, {touch, Key, Delay}, Config#cache_config.cluster_nodes)
				end);
		false -> ok
	end.

run_size(#cache_record{storage=DB}) -> g_storage:size(DB).

run_get_keys(#cache_record{storage=DB}) -> g_storage:keys(DB).

run_flush(Record=#cache_record{name=CacheName, config=Config}) ->
	Version = version(),
	api_flush(Version, Record),
	async:run(fun() ->
				cluster_notify(CacheName, {flush, Version}, Config#cache_config.cluster_nodes)
		end),
	ok.

run_foldl(Fun, Acc, #cache_record{storage=DB}) -> g_storage:foldl(DB, Fun, Acc).

% Server and API

sync(#cache_record{config=#cache_config{sync_mode=?LAZY_SYNC_MODE}}) -> ok;
sync(#cache_record{config=#cache_config{cluster_nodes=?CLUSTER_NODES_LOCAL}}) -> ok;
sync(Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) ->
	Fun = fun() ->
			Count = cluster_notify(Record#cache_record.name, {sync, self()}, Nodes),
			KeyList = receive_keys(Count),
			request_values(KeyList, Record)
	end,
	async:run(Fun);
sync(_Record) -> ok.

api_store(Key, Value, OldVersion, Delay, NewVersion, Record=#cache_record{config=Config, storage=DB}) ->
	case validate_change(DB, Key, OldVersion, NewVersion) of
		not_exists -> 
			insert(Key, Value, NewVersion, get_timeout(Delay, Config), Record),
			ok;
		{exists, StoredVersion} -> 
			delete(Key, StoredVersion, Record),
			insert(Key, Value, NewVersion, get_timeout(Delay, Config), Record),
			ok;
		Other -> Other
	end.

find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=?CLUSTER_NODES_LOCAL}}) -> 
	not_found;
find_value(_Key, #cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, sync_mode=?FULL_SYNC_MODE}}) -> 
	not_found;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=?NO_FUNCTION, cluster_nodes=Nodes}}) -> 
	case cluster_get(Key, Record#cache_record.name, Nodes) of
		{ok, Value, Version} ->
			async:run(fun() -> 
						api_store(Key, Value, ?NO_VERSION, ?USE_DEFAULT_EXPIRE, Version, Record) 
				end),
			{ok, Value, Version};
		not_found -> not_found
	end;
find_value(Key, Record=#cache_record{config=#cache_config{get_value_function=Function}}) ->
	try Function(Key) of
		not_found -> not_found;
		error -> error;
		Value -> 
			Version = version(),
			async:run(fun() -> 
						api_store(Key, Value, ?NO_VERSION, ?USE_DEFAULT_EXPIRE, Version, Record) 
				end),
			{ok, Value, Version}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.

touch_if_needed(_Key, ?NO_TOUCH, _Record) -> ok;
touch_if_needed(Key, Delay, Record) -> 
	run_touch(Key, Delay, Record).

api_remove(Key, OldVersion, NewVersion, Record=#cache_record{storage=DB}) ->
	case validate_change(DB, Key, OldVersion, NewVersion) of
		not_exists -> ok;
		{exists, StoredVersion} -> 
			delete(Key, StoredVersion, Record),
			ok;
		Other -> Other
	end.

api_flush(RefVersion, Record=#cache_record{storage=DB}) ->
	Fun = fun() ->
			Keys = g_storage:keys(DB, RefVersion),
			lists:foreach(fun({Key, Version}) ->
						delete(Key, Version, Record)
				end, Keys)
	end,
	async:run(Fun).

api_touch(_Key, _Delay, #cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> false;
api_touch(Key, Delay, #cache_record{config=Config, storage=DB}) ->
	Timeout = get_timeout(Delay, Config),
	g_storage:touch(DB, Key, Timeout).

purge(#cache_record{config=#cache_config{max_age=?NO_MAX_AGE}}) -> ok;
purge(Record=#cache_record{storage=DB}) ->
	Fun = fun() ->
			Now = current_time(),
			Keys = g_storage:expired(DB, Now),
			lists:foreach(fun({Key, Version}) ->
						delete(Key, Version, Record)
				end, Keys)
	end,
	async:run(Fun).

select(Key, #cache_record{storage=DB}) ->
	case g_storage:find(DB, Key) of
		{error, not_found} -> not_found;
		{ok, Value, Version, _Timeout} -> {ok, Value, Version}
	end.	

run_sync(Record, From) ->
	Fun = fun() ->
			Keys = run_get_keys(Record),
			From ! {cluster_msg, {keys, Keys}}
	end,
	async:run(Fun).

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
		{ok, Value, Version} -> api_store(Key, Value, ?NO_VERSION, ?USE_DEFAULT_EXPIRE, Version, Record);
		_ -> ok
	end,
	request_values(T, Record).

validate_change(DB, Key, OldVersion, NewVersion) ->
	case g_storage:find(DB, Key) of
		{error, not_found} -> validate_operation(OldVersion, ?NO_RECORD, NewVersion);
		{ok, _Value, StoredVersion, _Timeout} -> validate_operation(OldVersion, StoredVersion, NewVersion)
	end.

validate_operation(?NO_VERSION, ?NO_RECORD, _NewVersion) -> not_exists;
validate_operation(?NO_VERSION, StoredVersion, NewVersion) ->
	case StoredVersion =< NewVersion of
		true -> {exists, StoredVersion};
		_ -> {false, StoredVersion}
	end;
validate_operation(_OldVersion, ?NO_RECORD, _NewVersion) -> {false, invalid_version};
validate_operation(StoredVersion, StoredVersion, NewVersion) ->
	case StoredVersion =< NewVersion of
		true -> {exists, StoredVersion};
		_ -> {false, StoredVersion}
	end;
validate_operation(_OldVersion, _StoredVersion, _NewVersion) -> {false, invalid_version}.

get_timeout(_Delay, #cache_config{max_age=?NO_MAX_AGE}) -> 0;
get_timeout(?USE_DEFAULT_EXPIRE, #cache_config{max_age=Expire}) ->
	current_time() + Expire;
get_timeout(Delay, _Config) ->
	current_time() + Delay.

cluster_get(Key, CacheName, Nodes) ->
	Ref = make_ref(),
	RequestsSent = cluster_notify(CacheName, {get, Key, self(), Ref}, Nodes),
	receive_values(Ref, RequestsSent, 0).

receive_values(_Ref, Size, Size) -> not_found;
receive_values(Ref, Size, Count) ->
	receive
		{cluster_msg, {value, Ref, {ok, Value, Version}}} -> {ok, Value, Version};
		{cluster_msg, {value, Ref, _}} -> receive_values(Ref, Size, Count + 1);
		{cluster_msg, {value, _, _}} -> receive_values(Ref, Size, Count)
	after ?CLUSTER_TIMEOUT -> not_found
	end.

% Low level	

insert(Key, Value, Version, Timeout, Record=#cache_record{storage=DB}) ->
	Count = g_storage:insert(DB, Key, Value, Version, Timeout),
	update_counter(Record, Count).

delete(Key, Version, Record=#cache_record{storage=DB}) ->
	Count = g_storage:delete(DB, Key, Version),
	update_counter(Record, Count).

update_counter(Record=#cache_record{config=Config}, TableSize) ->
	case Config#cache_config.max_size of
		?NO_MAX_SIZE -> ok;
		MaxSize ->
			if 
				TableSize > MaxSize ->
					async:run(fun() ->
								delete_older(Record)
						end);
				true -> ok
			end
	end.

delete_older(Record=#cache_record{storage=DB}) ->
	case g_storage:older(DB) of
		{error, not_found} -> ok;
		{ok, Key, Version} -> delete(Key, Version, Record)
	end.

% System
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

stop_timer(?NO_TASK) -> ok;
stop_timer(Task) ->
	timer:cancel(Task).

% Util

brute_force(Fun, Version) ->
	case Fun(Version) of
		ok -> {ok, Version};
		{false, invalid_version} -> invalid_version;
		{false, OldVersion} -> brute_force(Fun, OldVersion + 1)
	end.

version() ->
	cclock:cluster_timestamp().

current_time() ->
	{Mega, Sec, _} = os:timestamp(),
	(Mega * 1000000) + Sec.

get_option_value(Tag, Options, Default) ->
	case lists:keyfind(Tag, 1, Options) of
		false -> Default;
		{_, Value} -> Value
	end.