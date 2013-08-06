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
-define(NO_COLUMBO, none).
-define(CLUSTER_TIMEOUT, 1000).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).
-export([store/3, get/2, remove/2, touch/2]).
-export([size/1]).
-export([purge/1]).

start_link(CacheConfig) when is_record(CacheConfig, cache_config)->
	gen_server:start_link(?MODULE, [CacheConfig], []).

store(Server, Key, Value) when is_atom(Server) ->
	gen_server:cast(Server, {store, Key, Value}).

get(Server, Key) when is_atom(Server) ->
	gen_server:call(Server, {get, Key}).

remove(Server, Key) when is_atom(Server) ->
	gen_server:cast(Server, {remove, Key}).

touch(Server, Key) when is_atom(Server) ->
	gen_server:cast(Server, {touch, Key}).

size(Server) when is_atom(Server) ->
	gen_server:call(Server, {size}).

purge(Server) when is_atom(Server) ->
	gen_server:cast(Server, {purge}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(db, {table, index}).
-record(state, {config, db, task, columbo, nodes}).

%% init
init([CacheConfig]) ->
	Table = create_table(CacheConfig),
	Index = create_index(CacheConfig),
	DB = #db{table=Table, index=Index},
	Task = start_timer(CacheConfig),
	{Columbo, Nodes} = start_columbo(CacheConfig),
	erlang:register(CacheConfig#cache_config.cache_name, self()),
	sync(DB, CacheConfig, Nodes),
	error_logger:info_msg("Cache ~p created on [~p]...\n", [CacheConfig#cache_config.cache_name, self()]),
	{ok, #state{config=CacheConfig, db=DB, task=Task, columbo=Columbo, nodes=Nodes}}.

%% handle_call
handle_call({get, Key}, From, State=#state{config=CacheConfig, db=DB, nodes=Nodes}) ->
	run_get(Key, DB, CacheConfig, Nodes, From),
	{noreply, State};

handle_call({size}, From, State=#state{config=CacheConfig, db=DB}) ->
	run_size(DB, CacheConfig, From),
	{noreply, State}.

%% handle_cast
handle_cast({store, Key, Value}, State=#state{config=CacheConfig, db=DB, nodes=Nodes}) ->
	run_store(Key, Value, DB, CacheConfig, true, Nodes),
	{noreply, State};

handle_cast({touch, Key}, State=#state{config=CacheConfig, db=DB, nodes=Nodes}) ->
	run_touch(Key, DB, CacheConfig, true, Nodes),
	{noreply, State};

handle_cast({remove, Key}, State=#state{config=CacheConfig, db=DB, nodes=Nodes}) ->
	run_remove(Key, DB, CacheConfig, true, Nodes),
	{noreply, State};

handle_cast({purge}, State=#state{config=CacheConfig, db=DB}) ->
	run_purge(DB, CacheConfig),
	{noreply, State}.

%% handle_info
handle_info({cluster_msg, {get, Key, From}}, State=#state{config=CacheConfig, db=DB}) ->
	run_cluster_get(Key, DB, CacheConfig, From),
	{noreply, State};

handle_info({cluster_msg, {store, Key, Value}}, State=#state{config=CacheConfig, db=DB}) ->
	run_store(Key, Value, DB, CacheConfig, false, []),
	{noreply, State};

handle_info({cluster_msg, {touch, Key}}, State=#state{config=CacheConfig, db=DB}) ->
	run_touch(Key, DB, CacheConfig, false, []),
	{noreply, State};

handle_info({cluster_msg, {remove, Key}}, State=#state{config=CacheConfig, db=DB}) ->
	run_remove(Key, DB, CacheConfig, false, []),
	{noreply, State};

handle_info({run_purge}, State=#state{config=CacheConfig, db=DB}) ->
	run_purge(DB, CacheConfig),
	{noreply, State};

handle_info({columbo, {new, _Service, _Ref, Node}}, State=#state{nodes=CurrentNodes}) ->
	{noreply, State#state{nodes=[Node|CurrentNodes]}};

handle_info({columbo, {remove, _Service, _Ref, Node}}, State=#state{nodes=CurrentNodes}) ->
	NCurrentNodes = lists:delete(Node, CurrentNodes),
	{noreply, State#state{nodes=NCurrentNodes}};

handle_info({change_nodes, Nodes}, State=#state{config=CacheConfig, columbo=Columbo}) ->
	NCacheConfig = CacheConfig#cache_config{nodes=Nodes},
	case Columbo of
		?NO_COLUMBO -> 
			{NColumbo, CurrentNodes} = start_columbo(NCacheConfig),
			{noreply, State#state{config=NCacheConfig, columbo=NColumbo, nodes=CurrentNodes}};
		_ -> 
			columbo:change_nodes(NCacheConfig#cache_config.cache_name, Columbo, Nodes),
			CurrentNodes= columbo:whereis_service(NCacheConfig#cache_config.cache_name, false),
			{noreply, State#state{config=NCacheConfig, nodes=CurrentNodes}}
	end;

handle_info({cluster_msg, {sync, From}}, State=#state{config=CacheConfig, db=DB}) ->
	run_sync(DB, CacheConfig, From),
	{noreply, State};

handle_info({stop_cache}, State) ->
	{stop, normal, State}.

%% terminate
terminate(_Reason, #state{config=CacheConfig, db=DB, task=Task, columbo=Columbo}) ->
	stop_columbo(Columbo, CacheConfig#cache_config.cache_name),
	stop_timer(Task),
	erlang:unregister(CacheConfig#cache_config.cache_name),
	drop(DB),
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

create_index(#cache_config{max_size=?NO_MAX_SIZE, purge=?NO_PURGE}) -> ?NO_INDEX_TABLE;
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

start_timer(#cache_config{purge=?NO_PURGE}) -> ?NO_TASK;
start_timer(#cache_config{purge=Interval}) ->
	TimerInterval = Interval * 1000,
	{ok, Task} = timer:send_interval(TimerInterval, {run_purge}),
	Task.

start_columbo(#cache_config{nodes=?CLUSTER_NODES_LOCAL}) -> {?NO_COLUMBO, []};
start_columbo(#cache_config{cache_name=CacheName, nodes=ClusterNodes}) ->
	Fun = fun(Msg) ->
			CacheName ! {columbo, Msg}
	end,
	Nodes = case ClusterNodes of
		?CLUSTER_NODES_ALL -> [];
		_ -> ClusterNodes
	end,
	columbo:request_notification(CacheName, Fun, Nodes).

sync(_DB, #cache_config{sync=?LAZY_SYNC_MODE}, _Nodes) -> ok;
sync(_DB, _CacheConfig, []) -> ok;
sync(DB, CacheConfig=#cache_config{function=?NO_FUNCTION}, Nodes) ->
	Fun = fun() ->
			cluster_notify(true, Nodes, {sync, self()}, CacheConfig),
			receive
				{cluster_msg, {keys, List}} -> request_values(List, DB, CacheConfig, Nodes)
			end
	end,
	spawn(Fun);
sync(_DB, _CacheConfig, _Nodes) -> ok.

request_values([], _DB, _CacheConfig, _Nodes) -> ok;
request_values([Key|T], DB, CacheConfig, Nodes) -> 
	case cluster_get(Key, CacheConfig, Nodes) of
		{ok, Value} -> insert(Key, Value, DB, CacheConfig, false, []);
		_ -> ok
	end,
	request_values(T, DB, CacheConfig, Nodes).

run_sync(#db{table=Table}, _CacheConfig, From) ->
	Fun = fun() ->
			Keys = ets:select(Table, [{{'$1','$2','$3','$4'},[],['$1']}]),
			From ! {cluster_msg, {keys, Keys}}
	end,
	spawn(Fun).

run_store(Key, Value, DB, CacheConfig, Notify, Nodes) ->
	Fun = fun() ->
			insert(Key, Value, DB, CacheConfig, Notify, Nodes)
	end,
	spawn(Fun).

run_touch(Key, #db{table=Table}, CacheConfig=#cache_config{expire=Expire}, Notify, Nodes) ->
	case Expire of
		?NO_MAX_AGE -> ok;
		_ -> 
			Fun = fun() ->
					Timeout = current_time() + Expire,
					case ets:update_element(Table, Key, {4, Timeout}) of
						true -> cluster_notify(Notify, Nodes, {touch, Key}, CacheConfig);
						false -> ok
					end
			end,
			spawn(Fun)
	end.	

insert(Key, Value, DB=#db{table=Table, index=Index}, CacheConfig=#cache_config{expire=Expire}, Notify, Nodes) ->
	Timeout = case Expire of
		?NO_MAX_AGE -> 0;
		_ -> current_time() + Expire
	end,
	Timestamp = insert_index(Key, Index),
	case ets:insert_new(Table, {Key, Value, Timestamp, Timeout}) of
		true -> 
			update_counter(DB, 1,  CacheConfig),
			cluster_notify(Notify, Nodes, {store, Key, Value}, CacheConfig);
		false -> 
			delete(Key, DB, CacheConfig, false, []),
			case ets:insert_new(Table, {Key, Value, Timestamp, Timeout}) of
				true -> 
					update_counter(DB, 1,  CacheConfig),
					cluster_notify(Notify, Nodes, {store, Key, Value}, CacheConfig);
				false -> ok
			end
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

cluster_notify(false, _Nodes, _Msg, _CacheConfig) -> ok;
cluster_notify(true, [], _Msg, _CacheConfig) -> ok;
cluster_notify(true, [Node|T], Msg, CacheConfig=#cache_config{cache_name=CacheName}) ->
	{CacheName, Node} ! {cluster_msg, Msg},
	cluster_notify(true, T, Msg, CacheConfig).

delete_older(#db{index=?NO_INDEX_TABLE}, _CacheConfig) -> ok;
delete_older(DB=#db{index=Index}, CacheConfig) ->
	case ets:first(Index) of
		'$end_of_table' -> ok;
		Timestamp -> 
			case ets:lookup(Index, Timestamp) of
				[] -> ok;
				[{_Timestamp, Key}] -> delete(Key, DB, CacheConfig, false, [])
			end
	end.

run_get(Key, DB, CacheConfig, Nodes, From)->
	Fun = fun() ->
			Reply = get(Key, DB, CacheConfig, Nodes, true),
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_cluster_get(Key, DB, CacheConfig, From) ->
	Fun = fun() ->
			Reply = get(Key, DB, CacheConfig, [], false),
			From ! {cluster_msg, {value, Reply}}
	end,
	spawn(Fun).

get(Key, DB=#db{table=Table}, CacheConfig=#cache_config{expire=Expire}, Nodes, UseCluster) ->
	case ets:lookup(Table, Key) of
		[] -> 
			case find_value(Key, DB, CacheConfig, Nodes, UseCluster) of
				{ok, Value} -> {ok, Value};
				Other -> Other
			end;
		[{_Key, Value, _Timestamp, Timeout}] -> 
			case Expire of
				?NO_MAX_AGE -> {ok, Value};
				_ -> 
					Now = current_time(),
					if 
						Now =< Timeout -> {ok, Value};
						true -> 
							case find_value(Key, DB, CacheConfig, Nodes, UseCluster) of
								{ok, Value} -> {ok, Value};
								Other -> 
									spawn(fun() -> delete(Key, DB, CacheConfig, false, []) end),
									Other
							end
					end
			end
	end.

run_size(#db{table=Table}, _CacheConfig, From)->
	Fun = fun() ->
			Reply = case ets:lookup(Table, ?RECORD_COUNTER) of
				[] -> 0;
				[{_Key, Count}] -> Count
			end,
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

find_value(_Key, _DB, #cache_config{function=?NO_FUNCTION}, [], _UseCluster) -> not_found;
find_value(_Key, _DB, #cache_config{function=?NO_FUNCTION}, _Nodes, false) -> not_found;
find_value(Key, DB, CacheConfig=#cache_config{function=?NO_FUNCTION}, Nodes, true) -> 
	case cluster_get(Key, CacheConfig, Nodes) of
		{ok, Value} ->
			spawn(fun() -> insert(Key, Value, DB, CacheConfig, false, []) end),
			{ok, Value};
		not_found -> not_found
	end;
find_value(Key, DB, CacheConfig=#cache_config{function=Function}, _Nodes, _UseCluster) ->
	try Function(Key) of
		Result -> 
			spawn(fun() -> insert(Key, Result, DB, CacheConfig, false, []) end),
			{ok, Result}
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Function, Key, Type, Error]),
			error
	end.	

cluster_get(Key, CacheConfig, Nodes) ->
	cluster_notify(true, Nodes, {get, Key, self()}, CacheConfig),
	receive_values(length(Nodes), 0).

receive_values(Size, Size) -> not_found;
receive_values(Size, Count) ->
	receive
		{cluster_msg, {value, {ok, Value}}} -> {ok, Value};
		{cluster_msg, {value, _}} -> receive_values(Size, Count + 1)
	after ?CLUSTER_TIMEOUT -> not_found
	end.

run_remove(Key, DB, CacheConfig, Notify, Nodes) ->
	Fun = fun() ->
			delete(Key, DB, CacheConfig, Notify, Nodes)
	end,
	spawn(Fun).

run_purge(#db{index=?NO_INDEX_TABLE}, _CacheConfig) -> ok;
run_purge(_DB, #cache_config{expire=?NO_MAX_AGE}) -> ok;
run_purge(DB=#db{index=Index}, CacheConfig=#cache_config{expire=Expire}) ->
	Fun = fun() ->
			{Mega, Sec, Micro} = timestamp(),
			Now = (Mega * 1000000) + Sec,
			ExpireDate = Now - Expire,
			NMega = ExpireDate div 1000000,
			NSec = ExpireDate rem 1000000,
			Match = [{{'$1','$2'},[{'<', '$1', {{NMega, NSec, Micro}}}],['$2']}],
			Keys = ets:select(Index, Match),
			delete_all(Keys, DB, CacheConfig)
	end,
	spawn(Fun).

delete_all([], _DB, _CacheConfig) -> ok;
delete_all([Key|T], DB, CacheConfig) ->
	delete(Key, DB, CacheConfig, false, []),
	delete_all(T, DB, CacheConfig).

delete(Key, DB=#db{table=Table}, CacheConfig, Notify, Nodes) ->
	case ets:lookup(Table, Key) of
		[] -> ok;
		[{_Key, _Value, Timestamp, _Timeout}] ->
			delete_index(Timestamp, DB),
			case ets:delete(Table, Key) of
				true -> 
					update_counter(DB, -1, CacheConfig),
					cluster_notify(Notify, Nodes, {remove, Key}, CacheConfig);
				false -> ok
			end
	end.

delete_index(_Timestamp, #db{index=?NO_INDEX_TABLE}) -> ok;
delete_index(Timestamp, #db{index=Index}) ->
	ets:delete(Index, Timestamp).

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

stop_timer(?NO_TASK) -> ok;
stop_timer(Task) ->
	timer:cancel(Task).

stop_columbo(?NO_COLUMBO, _CacheName) -> ok;
stop_columbo(Columbo, CacheName) -> 
	columbo:delete_notification(CacheName, Columbo).