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

-module(gibreel).

-behaviour(gen_server).

-include("gibreel.hrl").

-define(SERVER, {local, ?MODULE}).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([create_cache/1, create_cache/2, delete_cache/1]).
-export([list_caches/0]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

-spec create_cache(CacheName :: atom()) -> ok | {error, Reason :: any()}.
create_cache(CacheName) ->
	create_cache(CacheName, []).

-spec create_cache(CacheName :: atom(), Options :: [Option, ...]) -> ok | {error, Reason :: any()} 
	when Option :: {max_age, MaxAge :: pos_integer()}
	| {get_value_function, GetFunction :: fun((Key :: term()) -> FunReturn)}
						| {max_size, MaxSize :: pos_integer()}
						| {cluster_nodes, ClusterNodes :: local | all | [node(), ...]}
						| {purge_interval, PurgeInterval :: pos_integer()}
						| {sync_mode, SyncMode :: lazy | full},
						FunReturn :: term() | not_found | error.
create_cache(CacheName, Options) ->
	case create_cache_config(Options) of
		{ok, Config} -> gen_server:call(?MODULE, {create_cache, CacheName, Config});
		{error, Reason} -> {error, Reason}
	end.

-spec delete_cache(CacheName :: atom()) -> ok.
delete_cache(CacheName) ->
	gen_server:cast(?MODULE, {delete_cache, CacheName}).

-spec list_caches() -> [atom(), ...].
list_caches() ->
	ets:foldl(fun(#cache_record{name=Cache}, Acc) -> 
				[Cache|Acc] 
		end, [], ?GIBREEL_TABLE).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(state, {pids}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	create_table(),
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{pids=dict:new()}}.

%% handle_call
handle_call({create_cache, CacheName, CacheConfig}, _From, State=#state{pids=Pids}) ->
	case ets:lookup(?GIBREEL_TABLE, CacheName) of
		[] ->
			ets:insert(?GIBREEL_TABLE, #cache_record{name=CacheName, config=CacheConfig}),
			{ok, Pid} = g_cache_sup:start_cache(CacheName),
			erlang:monitor(process, Pid),
			NPids = dict:store(Pid, CacheName, Pids),
			{reply, ok, State#state{pids=NPids}};
		[_] -> {reply, ok, State}
	end.

handle_cast({delete_cache, CacheName}, State=#state{pids=Pids}) ->
	case whereis(CacheName) of
		undefined -> {noreply, State};
		Pid -> 
			Pid ! {stop_cache},
			ets:delete(?GIBREEL_TABLE, CacheName),
			NPids = dict:erase(Pid, Pids),
			{noreply, State#state{pids=NPids}}
	end.

%% handle_info
handle_info({'DOWN', _MonitorRef, process, Pid, shutdown}, State=#state{pids=Pids}) ->
	case dict:find(Pid, Pids) of
		error -> {noreply, State};
		{ok, CacheName} ->
			ets:delete(?GIBREEL_TABLE, CacheName),
			DPids = dict:erase(Pid, Pids),
			{noreply, State#state{pids=DPids}}
	end;

handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, State=#state{pids=Pids}) ->
	case dict:find(Pid, Pids) of
		error -> {noreply, State};
		{ok, CacheName} ->
			DPids = dict:erase(Pid, Pids),
			{ok, NPid} = g_cache_sup:start_cache(CacheName),
			erlang:monitor(process, NPid),
			NPids = dict:store(NPid, CacheName, DPids),
			{noreply, State#state{pids=NPids}}
	end.

%% terminate
terminate(_Reason, _State) ->
	drop_table(),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_cache_config(Options) ->
	Expire = proplists:get_value(max_age, Options, ?MAX_AGE_DEFAULT),
	Function = proplists:get_value(get_value_function, Options, ?FUNCTION_DEFAULT),
	MaxSize = proplists:get_value(max_size, Options, ?MAX_SIZE_DEFAULT),
	Nodes = proplists:get_value(cluster_nodes, Options, ?CLUSTER_NODES_DEFAULT),
	Purge = proplists:get_value(purge_interval, Options, ?PURGE_DEFAULT),
	Sync = proplists:get_value(sync_mode, Options, ?SYNC_MODE_DEFAULT),
	
	case validate_max_age(Expire) of
		ok -> 
			case validate_function(Function) of
				ok -> 
					case validate_max_size(MaxSize) of
						ok -> 
							case validate_nodes(Nodes) of
								ok -> 
									case validate_purge_interval(Purge, Expire) of
										ok -> 
											case validate_sync_mode(Sync) of
												ok -> 													
													{ok, #cache_config{
															max_age=Expire, 
															purge_interval=Purge, 
															get_value_function=Function, 
															max_size=MaxSize, 
															cluster_nodes=Nodes, 
															sync_mode=Sync}};
												Error -> {error, Error}
											end;
										Error -> {error, Error}
									end;
								Error -> {error, Error}
							end;
						Error -> {error, Error}
					end;
				Error -> {error, Error}
			end;
		Error -> {error, Error}
	end.

validate_max_age(Expite) when is_integer(Expite) -> ok;
validate_max_age(_Expite) -> "Max-Age must be an integer (seconds)".

validate_function(?NO_FUNCTION) -> ok;
validate_function(Function) when is_function(Function, 1) -> ok;
validate_function(_Function) -> "Get-Value-Function must be one function with arity 1".

validate_max_size(?NO_MAX_SIZE) -> ok;
validate_max_size(MaxSize) when is_integer(MaxSize) andalso MaxSize > 0 -> ok;
validate_max_size(_MaxSize) -> "Max-Size must be an integer and bigger than zero".

validate_nodes(?CLUSTER_NODES_LOCAL) -> ok;
validate_nodes(?CLUSTER_NODES_ALL) -> ok;
validate_nodes([]) -> "Empty list is not valid for Cluster-Nodes";
validate_nodes(Nodes) when is_list(Nodes) -> ok;
validate_nodes(_Nodes) -> "Cluster-Nodes must be a list of nodes or the values local or all".

validate_purge_interval(?NO_PURGE, _Expire) -> ok;
validate_purge_interval(_Purge, ?NO_MAX_AGE) -> "To use Purge-Interval you must use Max-Age";
validate_purge_interval(Purge, _Expire) when is_integer(Purge) andalso Purge > 0 -> ok; 
validate_purge_interval(_Purge, _Expire) -> "Purge-Interval must be an integer and bigger than zero (seconds)".

validate_sync_mode(?LAZY_SYNC_MODE) -> ok;
validate_sync_mode(?FULL_SYNC_MODE) -> ok;
validate_sync_mode(_Sync) -> "Sync-Mode must be lazy ou full".

create_table() ->
	Options = [set, public, named_table, {keypos, 2}, {read_concurrency, true}],
	ets:new(?GIBREEL_TABLE, Options).

drop_table() ->
	ets:delete(?GIBREEL_TABLE).