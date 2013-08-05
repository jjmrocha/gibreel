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

-include("gibreel.hlr").

-define(SERVER, {local, ?MODULE}).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).
-export([create_cache/1, create_cache/2, delete_cache/1]).
-export([list_caches/0]).
-export([add_node/2]).

start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

create_cache(CacheName) when is_atom(CacheName) ->
	create_cache(CacheName, []).

create_cache(CacheName, Options) when is_atom(CacheName) andalso is_list(Options) ->
	case create_cache_config(CacheName, Options) of
		{ok, Config} -> gen_server:call(?MODULE, {create_cache, CacheName, Config});
		{error, Reason} -> {error, Reason}
	end.

delete_cache(CacheName) when is_atom(CacheName) ->
	gen_server:cast(?MODULE, {delete_cache, CacheName}).

list_caches() ->
	gen_server:call(?MODULE, {list_caches}).

add_node(CacheName, Node) when is_atom(CacheName) andalso is_atom(Node) ->
	gen_server:cast(?MODULE, {add_node, CacheName, Node}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(state, {caches, pids}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{caches=dict:new(), pids=dict:new()}}.

%% handle_call
handle_call({list_caches}, From, State=#state{caches=Caches}) ->
	run_list_caches(Caches, From),
	{noreply, State};

handle_call({create_cache, CacheName, CacheConfig}, _From, State=#state{caches=Caches, pids=Pids}) ->
	case dict:find(CacheName, Caches) of
		error ->
			{ok, Pid} = g_cache_sup:start_cache(CacheConfig),
			erlang:monitor(process, Pid),
			NCaches = dict:store(CacheName, Pid, Caches),
			NPids = dict:store(Pid, CacheConfig, Pids),
			{reply, ok, State#state{caches=NCaches, pids=NPids}};
		{ok, _Pid} -> {reply, ok, State}
	end.

%% handle_cast
handle_cast({add_node, CacheName, Node}, State=#state{caches=Caches, pids=Pids}) ->
	case dict:find(CacheName, Caches) of
		error -> {noreply, State};
		{ok, Pid} -> 
			{ok, CacheConfig} = dict:find(Pid, Pids),
			case CacheConfig#cache_config.nodes of
				?CLUSTER_NODES_ALL -> {noreply, State};
				?CLUSTER_NODES_LOCAL -> 
					NPids = dict:store(Pid, CacheConfig#cache_config{nodes=[Node]}, Pids),
					{noreply, State#state{pids=NPids}};
				Nodes ->
					NNodes = [Node|Nodes],
					NPids = dict:store(Pid, CacheConfig#cache_config{nodes=NNodes}, Pids),
					CacheName ! {change_nodes, NNodes},
					{noreply, State#state{pids=NPids}}
			end
	end;

handle_cast({delete_cache, CacheName}, State=#state{caches=Caches, pids=Pids}) ->
	case dict:find(CacheName, Caches) of
		error -> {noreply, State};
		{ok, Pid} -> 
			CacheName ! {stop_cache},
			NCaches = dict:erase(CacheName, Caches),
			NPids = dict:erase(Pid, Pids),
			{noreply, State#state{caches=NCaches, pids=NPids}}
	end.

%% handle_info
handle_info({'DOWN', _MonitorRef, process, Pid, shutdown}, State=#state{caches=Caches, pids=Pids}) ->
	case dict:find(Pid, Pids) of
		error -> {noreply, State};
		{ok, CacheConfig} ->
			CacheName = CacheConfig#cache_config.cache_name,
			DCaches = dict:erase(CacheName, Caches),
			DPids = dict:erase(Pid, Pids),
			{noreply, State#state{caches=DCaches, pids=DPids}}
	end;

handle_info({'DOWN', _MonitorRef, process, Pid, _Reason}, State=#state{caches=Caches, pids=Pids}) ->
	case dict:find(Pid, Pids) of
		error -> {noreply, State};
		{ok, CacheConfig} ->
			CacheName = CacheConfig#cache_config.cache_name,
			DCaches = dict:erase(CacheName, Caches),
			DPids = dict:erase(Pid, Pids),
			{ok, NPid} = g_cache_sup:start_cache(CacheConfig),
			erlang:monitor(process, NPid),
			NCaches = dict:store(CacheName, NPid, DCaches),
			NPids = dict:store(NPid, CacheConfig, DPids),
			{noreply, State#state{caches=NCaches, pids=NPids}}
	end.

%% terminate
terminate(_Reason, _State) ->
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_cache_config(CacheName, Options) ->
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
													{ok, #cache_config{cache_name=CacheName, expire=Expire, purge=Purge, function=Function, max_size=MaxSize, nodes=Nodes, sync=Sync}};
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

run_list_caches(Caches, From) ->
	Fun = fun() ->
			Reply = dict:fetch_keys(Caches),
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).