
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

-module(gibreel_sup).

-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).

start_link() ->
	supervisor:start_link(?MODULE, []).

init([]) ->
	Gibreel = {g_cache,{g_cache, start_link, []}, permanent, 2000, worker, [g_cache]},
	{ok, {{one_for_one, 5, 60}, [Gibreel]}}.
