Gibreel
=======
*distributed cache implemented in Erlang*

Installation
------------

Using rebar:

```
{deps, [
	{gibreel, ".*", {hg, "https://bitbucket.org/jjmrocha/gibreel", "default"}}
]}.
```

Start Gibreel
-------------

Gibreel depends on columbo and cclock, you need to start both before gibreel.

```erlang
ok = application:start(columbo),
ok = application:start(cclock),
ok = application:start(gibreel).
```

gibreel Module
--------------

The module gibreel allows you to create and destroy caches.
The main functions are:

* create_cache/1  **To create a basic cache (using defaults)**
* create_cache/2  **To create a cache with options**
* delete_cache/1  **Delete cache**
* list_caches/0  **List all caches**

### create_cache Function

```erlang
gibreel:create_cache(CacheName :: atom()) -> 
	ok | {error, Reason :: any()}.

gibreel:create_cache(CacheName :: atom(), Options :: [Option, ...]) -> 
	ok | {error, Reason :: any()}.
```

The available options are:

* __max_age__ - It's a ``` pos_integer() ``` and represents the expiration time of the items stored in the cache, time in seconds, defaults to 0 (items on the cache don't expire)
* __get_value_function__ - It's a function ``` fun((Key :: term()) -> term() | not_found | error) ``` used by the cache to retrieve the value for the key, defaults to ``` none ```
* __max_size__ - It's a ``` pos_integer() ``` and represents the max number of items stored in the cache, when the max size is reached the oldest key-value is deleted to make space for the new one, defaults to ``` none ```
* __cluster_nodes__ - Must be one of the values ``` local | all | [node(), ...] ```, where (defaults to ``` local ```):
	* ``` local ``` - The cache is local
	* ``` all ``` - The cache must synchronize with all cache instances running in the cluster
    * ``` [node(), ...] ``` The cache must synchronize with all cache instances running on the node list
* __purge_interval__ - It's a ``` pos_integer() ``` and represents the running interval (time in seconds) for the purge process, which removes expired items from the cache, defaults to ``` none ``` (purge process don't run)
* __sync_mode__ - Must be one of the values ``` lazy | full ```, where (defaults to ``` lazy ```):
    * ``` lazy ``` - Cache only synchronize the new key-values or when a item is removed
    * ``` full ``` - Cache must synchronize with all cache instances at start

### delete_cache Function

```erlang
gibreel:delete_cache(CacheName :: atom()) -> ok.
```

Deletes the cache.

### list_caches Function

```erlang
gibreel:list_caches() -> [atom(), ...].
```

Lists the names of all caches.

### Examples

#### Using gibreel as a session storage

The webapp server [Kill Bill](https://bitbucket.org/jjmrocha/kill-bill) uses Gibreel to store the session data.

```erlang
Options = [
    {max_age, 1200},
	{purge_interval, 60},
	{cluster_nodes, all},
	{sync_mode, full}
],
gibreel:create_cache(my_webapp_session, Options).
```

#### Using gibreel as a DB cache

The site [Gmailbox.org](https://www.gmailbox.org) uses Gibreel as a DB cache for CouchDB.

```erlang
LoadFunction = fun(Key) ->
	case chair:get_doc(gmailboxDB, Key) of
		{ok, Doc} -> Doc;
		{db_error, Error} ->
			case jsondoc:get_value(<<"error">>, Error) of
				<<"not_found">> -> not_found;
				_ -> error
			end;
		{error, _} -> error		
	end
end,
Options = [
	{get_value_function, LoadFunction},
	{max_age, 600},
	{max_size, 10000},
	{cluster_nodes, all}
],
gibreel:create_cache(account_cache, Options).
```

g_cache Module
--------------

The module cache allows you to store, remove and retrieve items from cache.
The main functions are:

* store/3  **Stores a key-value pair on the cache**
* get/2  **Retrieves the value for the key**
* remove/2  **Removes a key-value from the cache**
* touch/2  **Changes the item expiration date**
* size/1  **Return the number o key-value pairs in the cache**
* foldl/3  **Calls a function for all elements of the cache and returns an accumulator**

### store Function

```erlang
g_cache:store(CacheName :: atom(), Key :: term(), Value :: term()) -> 
    no_cache | ok.
```

Stores a key-value pair on the cache.

### get Function

```erlang
g_cache:get(CacheName :: atom(), Key :: term()) -> 
    {ok, Value :: term()} | not_found | no_cache | error.
```

Retrieves the value for the key.

### remove Function

```erlang
g_cache:remove(CacheName :: atom(), Key :: term()) -> 
    no_cache | ok.
```

Removes a key-value from the cache.

### touch Function

```erlang
g_cache:touch(CacheName :: atom(), Key :: term()) -> ok.
```

Changes the item expiration date.

### size Function

```erlang
g_cache:size(CacheName :: atom()) -> no_cache | integer().
```

Return the number o key-value pairs in the cache.

### foldl Function

```erlang
g_cache:foldl(CacheName :: atom(), Fun, Acc :: term()) -> no_cache | term().
```

Calls function Fun on successive elements on the cache, starting with Acc. Fun/2 must return a new accumulator which is passed to the next call. The function returns the final value of the accumulator. Acc is returned if the list is empty.

For example:

```erlang
g_cache:foldl(example, fun({_Key, Value}, Acc) ->
    [Value|Acc]
end, []).
```
