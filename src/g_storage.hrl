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

-define(RECORD_COUNTER, gcache_record_counter).

-define(NO_INDEX_TABLE, none).
-record(cache_storage, {table, index=?NO_INDEX_TABLE}).

-define(DATA_RECORD(Key, Value, Version, Timeout), {Key, Value, Version, Timeout}).
-define(INDEX_RECORD(Version, Key), {Version, Key}).