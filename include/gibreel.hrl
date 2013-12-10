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

-define(CLUSTER_NODES_LOCAL, local).
-define(CLUSTER_NODES_ALL, all).

-define(LAZY_SYNC_MODE, lazy).
-define(FULL_SYNC_MODE, full).

-define(NO_MAX_AGE, 0).
-define(NO_MAX_SIZE, none).
-define(NO_FUNCTION, none).
-define(NO_PURGE, none).

-define(MAX_AGE_DEFAULT, ?NO_MAX_AGE).
-define(FUNCTION_DEFAULT, ?NO_FUNCTION).
-define(MAX_SIZE_DEFAULT, ?NO_MAX_SIZE).
-define(CLUSTER_NODES_DEFAULT, ?CLUSTER_NODES_LOCAL).
-define(PURGE_DEFAULT, ?NO_PURGE).
-define(SYNC_MODE_DEFAULT, ?LAZY_SYNC_MODE).

-define(NO_INDEX_TABLE, none).
-define(NO_DATA, none).

-record(cache_config, {max_age, purge_interval, get_value_function, max_size, cluster_nodes, sync_mode}).
-record(cache_memory, {table, index=?NO_INDEX_TABLE}).
-record(cache_record, {name, config, memory=?NO_DATA}).

-define(GIBREEL_TABLE, gibreel).