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
-export([list_caches/0, find_cache/1]).
-export([store/3, get/2, remove/2]).

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

find_cache(CacheName) when is_atom(CacheName) ->
	gen_server:call(?MODULE, {find_cache, CacheName}).

store(Server, Key, Value) when is_pid(Server) ->
	g_cache:store(Server, Key, Value);
store(CacheName, Key, Value) when is_atom(CacheName) ->
	case find_cache(CacheName) of
		{ok, Server} -> store(Server, Key, Value);
		error -> cache_not_found
	end.

get(Server, Key) when is_pid(Server) ->
	g_cache:get(Server, Key);
get(CacheName, Key) when is_atom(CacheName) ->
	case find_cache(CacheName) of
		{ok, Server} -> get(Server, Key);
		error -> cache_not_found
	end.

remove(Server, Key) when is_pid(Server) ->
	g_cache:remove(Server, Key);
remove(CacheName, Key) when is_atom(CacheName) ->
	case find_cache(CacheName) of
		{ok, Server} -> remove(Server, Key);
		error -> cache_not_found
	end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

-record(state, {caches}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	error_logger:info_msg("~p starting on [~p]...\n", [?MODULE, self()]),
	{ok, #state{caches=dict:new()}}.

%% handle_call
handle_call({find_cache, CacheName}, From, State=#state{caches=Caches}) ->
	run_find_cache(CacheName, Caches, From),
	{noreply, State};

handle_call({list_caches}, From, State=#state{caches=Caches}) ->
	run_list_caches(Caches, From),
	{noreply, State};

handle_call({create_cache, CacheName, CacheConfig}, _From, State=#state{caches=Caches}) ->
	case dict:find(CacheName, Caches) of
		error ->
			{ok, Pid} = g_cache_sup:start_cache(CacheConfig),
			NCaches = dict:store(CacheName, {ok, Pid}, Caches),
			{reply, Pid, State#state{caches=NCaches}};
		{ok, Pid} -> {reply, {ok, Pid}, State}
	end.

%% handle_cast
handle_cast({delete_cache, CacheName}, State=#state{caches=Caches}) ->
	case dict:find(CacheName, Caches) of
		error -> {noreply, State};
		{ok, Pid} -> 
			g_cache:stop(Pid),
			NCaches = dict:erase(CacheName, Caches),
			{noreply, State#state{caches=NCaches}}
	end.

%% handle_info
handle_info(Info, State) ->
	error_logger:info_msg("handle_info(~p)\n", [Info]),
	{noreply, State}.

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
	Function = proplists:get_value(function, Options, ?FUNCTION_DEFAULT),
	MaxSize = proplists:get_value(max_size, Options, ?MAX_SIZE_DEFAULT),
	Nodes = proplists:get_value(cluster_nodes, Options, ?CLUSTER_NODES_DEFAULT),
	Prune = proplists:get_value(prune_interval, Options, ?PRUNE_DEFAULT),
	
	case validate_max_age(Expire) of
		ok -> 
			case validate_function(Function) of
				ok -> 
					case validate_max_size(MaxSize) of
						ok -> 
							case validate_nodes(Nodes) of
								ok -> 
									case validate_prune_interval(Prune) of
										ok -> 
											{ok, #cache_config{cache_name=CacheName, expire=Expire, prune=Prune, function=Function, max_size=MaxSize, nodes=Nodes}};
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

validate_function(?FUNCTION_DEFAULT) -> ok;
validate_function(Function) when is_function(Function, 1) -> ok;
validate_function(_Function) -> "Function mas be an function with arity 1".

validate_max_size(?MAX_SIZE_DEFAULT) -> ok;
validate_max_size(MaxSize) when is_integer(MaxSize) andalso MaxSize > 0 -> ok;
validate_max_size(_MaxSize) -> "Max-Size must be an integer and bigger than zero".

validate_nodes(?CLUSTER_NODES_LOCAL) -> ok;
validate_nodes(?CLUSTER_NODES_ALL) -> ok;
validate_nodes([]) -> "Empty list is not valid for Cluster-Nodes";
validate_nodes(Nodes) when is_list(Nodes) -> ok;
validate_nodes(_Nodes) -> "Cluster-Nodes must be a list of nodes or the values local or all".

validate_prune_interval(?PRUNE_DEFAULT) -> ok;
validate_prune_interval(Prune) when is_integer(Prune) andalso Prune > 0 -> ok; 
validate_prune_interval(_Prune) -> "Prune-Interval must be an integer and bigger than zero (seconds)".

run_find_cache(CacheName, Caches, From) ->
	Fun = fun() ->
			Reply = dict:find(CacheName, Caches),
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_list_caches(Caches, From) ->
	Fun = fun() ->
			Reply = dict:fetch_keys(Caches),
			gen_server:reply(From, Reply)
	end,
	spawn(Fun).