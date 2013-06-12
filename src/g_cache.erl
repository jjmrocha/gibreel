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

-define(SERVER, {local, ?MODULE}).

-define(TYPE_STATIC, static_cache).
-define(TYPE_DYNAMIC, dynamic_cache).

-define(NO_VALUE, null).

-record(cache_id, {
	cache :: term(), 
	key   :: term()
}).
					
-record(cache_type, {
	cache                  :: term(), 
	type                   :: atom(), 
	function               :: fun(), 
	max_age=3600           :: pos_integer(), 
	max_size=10000         :: pos_integer(), 
	record_count=0         :: integer(),
 	first_record=?NO_VALUE :: #cache_id{} | atom(),
 	last_record=?NO_VALUE  :: #cache_id{} | atom()
}).
						
-record(cache_value, {
	cache_id              :: #cache_id{}, 
	value                 :: term(), 
	cache_date            :: calendar:datetime(),
  	previous_id=?NO_VALUE :: #cache_id{} | atom(),
  	next_id=?NO_VALUE     :: #cache_id{} | atom()
}).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/0, create_cache/4, create_cache/1, delete_cache/1, exist_cache/1, purge_cache/2]).
-export([delete/2, store/3, get/2]).

%% ====================================================================
%% Client functions
%% ====================================================================
start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

create_cache(Cache, Fun, MaxAge, MaxSize) when is_function(Fun, 1) andalso MaxSize > 0 andalso MaxAge > 0 ->
	gen_server:call(?MODULE, {create_cache, Cache, Fun, MaxAge, MaxSize}).

create_cache(Cache) ->
	gen_server:call(?MODULE, {create_cache, Cache}).

delete_cache(Cache) ->
	gen_server:cast(?MODULE, {delete_cache, Cache}).

exist_cache(Cache) ->
	gen_server:call(?MODULE, {exist_cache, Cache}).

purge_cache(Cache, Age) when is_integer(Age) ->
   gen_server:cast(?MODULE, {purge_cache, Cache, Age}).

get(Cache, Key) ->
	gen_server:call(?MODULE, {get, Cache, Key}).

delete(Cache, Key) ->
	gen_server:call(?MODULE, {delete, Cache, Key}).

store(Cache, Key, Value) ->
	gen_server:call(?MODULE, {store, Cache, Key, Value}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
init([]) ->
	process_flag(trap_exit, true),
	error_logger:info_msg("Gibreel Farishta Cache System starting on [~p]...\n", [self()]),
	init_storage(),
	{ok, []}.

handle_call({create_cache, Cache, Fun, MaxAge, MaxSize}, From, State) ->
	run_create_cache(#cache_type{cache=Cache, type=?TYPE_DYNAMIC, function=Fun, max_age=MaxAge, max_size=MaxSize}, From),
	{noreply, State};
handle_call({create_cache, Cache}, From, State) ->
	run_create_cache(#cache_type{cache=Cache, type=?TYPE_STATIC}, From),
	{noreply, State};
handle_call({exist_cache, Cache}, From, State) ->
    run_exist_cache(Cache, From),
    {noreply, State};
handle_call({get, Cache, Key}, From, State) ->
	run_get(#cache_id{cache=Cache, key=Key}, From),
	{noreply, State};
handle_call({delete, Cache, Key}, From, State) ->
	run_delete(#cache_id{cache=Cache, key=Key}, From),
	{noreply, State};
handle_call({store, Cache, Key, Value}, From, State) ->
	run_store(#cache_id{cache=Cache, key=Key}, Value, From),
	{noreply, State}.

handle_cast({delete_cache, Cache}, State) ->
	run_delete_cache(Cache),
	{noreply, State};
handle_cast({purge_cache, Cache, Age}, State) ->
	run_purge_cache(Cache, Age),
	{noreply, State}.

handle_info(Info, State) ->
	error_logger:info_msg("handle_info(~p)\n", [Info]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Server functions
%% ====================================================================
run_create_cache(CacheType, From) ->
	Fun = fun () ->
		Reply = case find_cache_type(CacheType#cache_type.cache) of
			aborted -> error;
			not_found -> 
				case create_cache_type(CacheType) of
					aborted -> error;
					_ -> ok
				end;				
			_ -> duplicated
		end,				   
		gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_delete_cache(Cache) ->
	Fun = fun () -> delete_cache_type(Cache) end,
	spawn(Fun).

run_purge_cache(Cache, Age) ->
	Fun = fun () -> purge_cache_value(Cache, Age) end,
	spawn(Fun).

run_exist_cache(Cache, From) ->
	Fun = fun () ->
		Reply = case find_cache_type(Cache) of
			aborted -> error;
			not_found -> false;
			_ -> true
		end,
		gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_get(CacheId, From) ->
	Fun = fun () ->
		Reply = case get_cache_value(CacheId) of
			aborted -> error;
			Value -> Value
		end,
		gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_delete(CacheId, From) ->
	Fun = fun() ->
		Reply = case delete_cache_value(CacheId) of
			aborted -> error;
			_ -> ok
		end,
		gen_server:reply(From, Reply)
	end,
	spawn(Fun).

run_store(CacheId, Value, From) ->
	CacheValue = #cache_value{cache_id=CacheId, value=Value},
	Fun = fun() ->
		Reply = case update_cache_value(CacheValue) of
			aborted -> error;
			_ -> ok
		end,
		gen_server:reply(From, Reply)		  
	end,
	spawn(Fun).

%% ====================================================================
%% Private functions
%% ====================================================================
get_cache_value(CacheId) ->
	CacheValue = find_cache_value(CacheId),
	value(CacheId, CacheValue).

value(_CacheId, aborted) -> error;
value(CacheId = #cache_id{cache=Cache}, not_found) ->
	case find_cache_type(Cache) of
		aborted -> not_found;
		not_found -> not_found;
		CacheType ->
			case CacheType#cache_type.type of
				?TYPE_STATIC -> not_found;
				?TYPE_DYNAMIC -> execute_and_update(CacheId, CacheType#cache_type.function)
			end
	end;
value(CacheId = #cache_id{cache=Cache}, #cache_value{value=Value, cache_date=CacheDate}) ->
	case find_cache_type(Cache) of
		aborted -> {Value, CacheDate};
		not_found ->
			delete_cache_value(CacheId),
			not_found;
		CacheType ->
			case CacheType#cache_type.type of
				?TYPE_STATIC -> {Value, CacheDate};
				?TYPE_DYNAMIC -> 
					ExpireDate = calendar:datetime_to_gregorian_seconds(CacheDate) + CacheType#cache_type.max_age,
					Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
					if 
						ExpireDate >= Now -> {Value, CacheDate};
						true -> execute_and_update(CacheId, CacheType#cache_type.function)
					end
			end
	end.
	
execute_and_update(CacheId = #cache_id{key=Key}, Fun) ->
	try Fun(Key) of
		Result ->
			case update_cache_value(#cache_value{cache_id=CacheId, value=Result}) of
				aborted -> {Result, calendar:local_time()};
				Saved -> {Saved#cache_value.value, Saved#cache_value.cache_date}
			end
	catch 
		Type:Error -> 
			error_logger:error_msg("~p(~p) [~p:~p]\n", [Fun, Key, Type, Error]),
			error
	end.

%% ====================================================================
%% Finder functions
%% ====================================================================
find_cache_type(Id) ->
	Fun = fun() ->
		case mnesia:read(cache_type, Id) of
			[] -> not_found;
			[Record] -> Record
		end
	end,
	case mnesia:transaction(Fun) of
		{atomic, Record} -> Record;
		{aborted, Reason} -> 
			error_logger:error_msg("find_cache_type(~p) - [~p]\n" , [Id, Reason]),
			aborted
	end.

find_cache_value(CacheId) ->
	Fun = fun() ->
		case mnesia:read(cache_value, CacheId) of
			[] -> not_found;
			[Record] -> Record
		end
	end,
	case mnesia:transaction(Fun) of
		{atomic, Record} -> Record;
		{aborted, Reason} -> 
			error_logger:error_msg("find_cache_value(~p) - [~p]\n" , [CacheId, Reason]),
			aborted
	end.

%% ====================================================================
%% DB functions
%% ====================================================================
init_storage() ->
	Nodes = case application:get_key(gibreel, cache_nodes) of
		{ok, Value} -> Value;
		undefined -> [node()]
	end,
	try mnesia:table_info(cache_type, type)
	catch
		exit:_ ->
			mnesia:create_table(cache_type, [{attributes, record_info(fields, cache_type)},
				{ram_copies, Nodes}]),
			mnesia:create_table(cache_value, [{attributes, record_info(fields, cache_value)},
				{ram_copies, Nodes}])	
	end.

create_cache_type(Record) ->
	Fun = fun() ->
		mnesia:write(Record),
		Record
	end,
	case mnesia:transaction(Fun) of
		{atomic, Saved} -> Saved;
		{aborted, Reason} -> 
			error_logger:error_msg("create_cache_type(~p) - [~p]\n" , [Record, Reason]),
			aborted
	end.

delete_cache_type(Id) ->
	Fun = fun() ->
		case mnesia:read(cache_type, Id) of
			[] -> ok;
			[_Record] -> 
				mnesia:delete(cache_type, Id, write),
				F = fun(X, _) ->
					CacheId = X#cache_value.cache_id,
					if 
						CacheId#cache_id.cache =:= Id ->
							mnesia:delete_object(X),
							[];
						true -> []
					end
				end,
				mnesia:foldl(F, [], cache_value)				
		end				  
	end,
	case mnesia:transaction(Fun) of
		{atomic, _} -> ok;
		{aborted, Reason} -> 
			error_logger:error_msg("delete_cache_type(~p) - [~p]\n" , [Id, Reason]),
			aborted
	end.

purge_cache_value(Id, Age) ->
	Fun = fun() ->
		case mnesia:read(cache_type, Id) of
			[] -> ok;
			[_Record] -> 
				ExpireDate = calendar:datetime_to_gregorian_seconds(calendar:local_time()) - Age,
				F = fun(X, _) ->
					CacheId = X#cache_value.cache_id,
					if 
						CacheId#cache_id.cache =:= Id ->
							CacheDate = calendar:datetime_to_gregorian_seconds(X#cache_value.cache_date),
							if 
								CacheDate < ExpireDate -> 
									delete_cache_value(CacheId),
									[];
								true -> []
							end;
						true -> []
					end
				end,
				mnesia:foldl(F, [], cache_value)				
		end				  
	end,
	case mnesia:transaction(Fun) of
		{atomic, _} -> ok;
		{aborted, Reason} -> 
			error_logger:error_msg("purge_cache_value(~p, ~p) - [~p]\n" , [Id, Age, Reason]),
			aborted
	end.

update_cache_value(Record) ->
	Fun = fun() ->
		CacheId = Record#cache_value.cache_id,
		case mnesia:read(cache_type, CacheId#cache_id.cache) of
			[] ->
				Reason = io_lib:format("Cache ~p not found!", [CacheId#cache_id.cache]),
				mnesia:abort(Reason);
			[CacheType] ->
				case mnesia:read(cache_value, CacheId) of
					[] -> 
						NewCacheType = new_pointers(CacheType#cache_type.type, CacheType, CacheId),
						case CacheType#cache_type.type of
							?TYPE_DYNAMIC ->
								if
									NewCacheType#cache_type.record_count > NewCacheType#cache_type.max_size ->
										delete_cache_value(NewCacheType#cache_type.first_record);
									true -> ok
								end,
                        			NewRecord = Record#cache_value{cache_date = erlang:localtime(), previous_id=CacheType#cache_type.last_record},
                  				mnesia:write(NewRecord),
                  				NewRecord;
               				?TYPE_STATIC ->
                  				NewRecord = Record#cache_value{cache_date = erlang:localtime()},
                  				mnesia:write(NewRecord),
                  				NewRecord
            			end;
					[OldValue] -> 
						PreviousId = update_pointers(CacheType#cache_type.type, CacheType, OldValue, CacheId),
						NewRecord = Record#cache_value{cache_date = erlang:localtime(), previous_id=PreviousId},
						mnesia:write(NewRecord),
						NewRecord
				end
		end	
	end,
	case mnesia:transaction(Fun) of
		{atomic, Saved} -> Saved;
		{aborted, Reason} -> 
			error_logger:error_msg("update_cache_value(~p) - [~p]\n" , [Record, Reason]),
			aborted
	end.

delete_cache_value(CacheId) ->
	Fun = fun() ->
		case mnesia:read(cache_value, CacheId, write) of
			[]-> ok;
			[OldRecord] ->
				mnesia:delete(cache_value, CacheId, write),
				[CacheType] = mnesia:read(cache_type, CacheId#cache_id.cache, write),
				delete_pointers(CacheType#cache_type.type, CacheType, OldRecord)
		end
	end,
	case mnesia:transaction(Fun) of
		{atomic, _} -> ok;
		{aborted, Reason} -> 
			error_logger:error_msg("delete_cache_value(~p) - [~p]\n" , [CacheId, Reason]),
			aborted
	end.

%% ====================================================================
%% DB Secret functions
%% ====================================================================
delete_pointers(?TYPE_STATIC, CacheType, _OldRecord) ->
	mnesia:write(CacheType#cache_type{record_count=CacheType#cache_type.record_count - 1});
delete_pointers(?TYPE_DYNAMIC, CacheType, OldRecord) ->
	case OldRecord#cache_value.previous_id of
		?NO_VALUE ->
			case OldRecord#cache_value.next_id of
				?NO_VALUE -> 
					mnesia:write(CacheType#cache_type{first_record=?NO_VALUE, last_record=?NO_VALUE, record_count=0});
				NextId -> 
					mnesia:write(CacheType#cache_type{first_record=NextId, record_count=CacheType#cache_type.record_count - 1}),
					[NextRecord] = mnesia:read(cache_value, NextId, write),
					mnesia:write(NextRecord#cache_value{previous_id=?NO_VALUE})
			end;
		PreviousId ->
			case OldRecord#cache_value.next_id of
				?NO_VALUE -> 
					mnesia:write(CacheType#cache_type{last_record=PreviousId, record_count=CacheType#cache_type.record_count - 1}),
					[PreviousRecord] = mnesia:read(cache_value, PreviousId, write),
					mnesia:write(PreviousRecord#cache_value{next_id=?NO_VALUE});
				NextId -> 
					mnesia:write(CacheType#cache_type{record_count=CacheType#cache_type.record_count - 1}),
					[NextRecord] = mnesia:read(cache_value, NextId, write),
					[PreviousRecord] = mnesia:read(cache_value, PreviousId, write),
					mnesia:write(NextRecord#cache_value{previous_id=PreviousId}),
					mnesia:write(PreviousRecord#cache_value{next_id=NextId})
			end
	end.

new_pointers(?TYPE_STATIC, CacheType, _CacheId) ->
	NewCacheType = CacheType#cache_type{record_count=CacheType#cache_type.record_count + 1},
	mnesia:write(NewCacheType),
	NewCacheType;
new_pointers(?TYPE_DYNAMIC, CacheType, CacheId) ->
	case CacheType#cache_type.first_record of
		?NO_VALUE -> 
			NewCacheType = CacheType#cache_type{first_record=CacheId, last_record=CacheId, record_count=1},
			mnesia:write(NewCacheType),
			NewCacheType;
		_ ->
			[LastRecord] = mnesia:read(cache_value, CacheType#cache_type.last_record, write),
			mnesia:write(LastRecord#cache_value{next_id=CacheId}),
			NewCacheType = CacheType#cache_type{last_record=CacheId, record_count=CacheType#cache_type.record_count + 1},
			mnesia:write(NewCacheType),
			NewCacheType
	end.

update_pointers(?TYPE_STATIC, _CacheType, _OldRecord, _CacheI) -> ?NO_VALUE;
update_pointers(?TYPE_DYNAMIC, CacheType, OldRecord, CacheId) ->
	case OldRecord#cache_value.previous_id of
		?NO_VALUE ->
			case OldRecord#cache_value.next_id of
				?NO_VALUE -> ?NO_VALUE;
				NextId -> 
					[NextRecord] = mnesia:read(cache_value, NextId, write),
					mnesia:write(NextRecord#cache_value{previous_id=?NO_VALUE}),
					[LastRecord] = mnesia:read(cache_value, CacheType#cache_type.last_record, write),
					mnesia:write(LastRecord#cache_value{next_id=CacheId}),
					mnesia:write(CacheType#cache_type{first_record=NextId, last_record=CacheId}),
					CacheType#cache_type.last_record
			end;
		PreviousId ->
			case OldRecord#cache_value.next_id of
				?NO_VALUE -> PreviousId;
				NextId -> 
					[NextRecord] = mnesia:read(cache_value, NextId, write),
					[PreviousRecord] = mnesia:read(cache_value, PreviousId, write),
					mnesia:write(NextRecord#cache_value{previous_id=PreviousId}),
					mnesia:write(PreviousRecord#cache_value{next_id=NextId}),
					[LastRecord] = mnesia:read(cache_value, CacheType#cache_type.last_record, write),
					mnesia:write(LastRecord#cache_value{next_id=CacheId}),
					mnesia:write(CacheType#cache_type{last_record=CacheId}),
					CacheType#cache_type.last_record
			end
	end.
