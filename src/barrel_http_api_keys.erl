%%%-------------------------------------------------------------------
%%% @doc API Key management for barrel_docdb HTTP authentication
%%%
%%% Provides authentication via API keys stored in DETS. Features:
%%% - Bearer token authentication
%%% - Admin key bootstrap on first startup
%%% - Environment variable override (BARREL_DOCDB_ADMIN_KEY)
%%% - Permission-based access control
%%%
%%% == Quick Start ==
%%% ```
%%% %% Validate a key
%%% {ok, KeyInfo} = barrel_http_api_keys:validate_key(<<"ak_xyz...">>).
%%%
%%% %% Create a new key
%%% {ok, Key, KeyInfo} = barrel_http_api_keys:create_key(#{
%%%     name => <<"my-app">>,
%%%     permissions => [<<"read">>, <<"write">>]
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_api_keys).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    stop/0,
    validate_key/1,
    validate_key/2,
    create_key/1,
    delete_key/1,
    list_keys/0,
    has_any_keys/0,
    generate_admin_key/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TABLE, barrel_http_api_keys).

%% API key record
-record(api_key, {
    key_hash :: binary(),          %% SHA-256 hash of the key
    key_prefix :: binary(),        %% First 8 chars for identification
    name :: binary(),
    permissions :: [binary()],
    databases :: all | [binary()], %% 'all' for global, or list of db names
    is_admin :: boolean(),
    created_at :: integer(),       %% Unix timestamp milliseconds
    last_used :: integer() | undefined
}).

-record(state, {
    table_ref :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the API keys manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Stop the API keys manager
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Validate an API key (global access check)
%% Returns key info if valid, error otherwise
-spec validate_key(binary()) -> {ok, map()} | {error, invalid_key}.
validate_key(Key) ->
    gen_server:call(?SERVER, {validate_key, Key}).

%% @doc Validate an API key for a specific database
%% Returns key info if valid and has access to the database, error otherwise
-spec validate_key(binary(), binary()) -> {ok, map()} | {error, invalid_key | access_denied}.
validate_key(Key, DbName) ->
    gen_server:call(?SERVER, {validate_key, Key, DbName}).

%% @doc Create a new API key
%% Options:
%%   - name: Key name (required)
%%   - permissions: List of permission binaries (default: `[&lt;&lt;"read"&gt;&gt;]')
%%   - databases: 'all' for global access, or list of db names (default: all)
%%   - is_admin: Whether this is an admin key (default: false)
-spec create_key(map()) -> {ok, binary(), map()} | {error, term()}.
create_key(Opts) ->
    gen_server:call(?SERVER, {create_key, Opts}).

%% @doc Delete an API key by its prefix
-spec delete_key(binary()) -> ok | {error, term()}.
delete_key(KeyPrefix) ->
    gen_server:call(?SERVER, {delete_key, KeyPrefix}).

%% @doc List all API keys (metadata only, no secrets)
-spec list_keys() -> {ok, [map()]}.
list_keys() ->
    gen_server:call(?SERVER, list_keys).

%% @doc Check if any API keys exist
-spec has_any_keys() -> boolean().
has_any_keys() ->
    gen_server:call(?SERVER, has_any_keys).

%% @doc Generate a new admin key
-spec generate_admin_key() -> {ok, binary()} | {error, term()}.
generate_admin_key() ->
    gen_server:call(?SERVER, generate_admin_key).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Get data path from application config
    DataPath = application:get_env(barrel_docdb, data_path, "data/barrel_docdb"),
    KeysFile = filename:join(DataPath, "api_keys.dets"),

    %% Ensure directory exists
    ok = filelib:ensure_dir(KeysFile),

    %% Open DETS table
    case dets:open_file(?TABLE, [{file, KeysFile}, {keypos, #api_key.key_hash}]) of
        {ok, _TableRef} ->
            State = #state{},
            %% Bootstrap admin key if needed
            ok = maybe_bootstrap_admin_key(State),
            {ok, State};
        {error, Reason} ->
            {stop, {dets_open_failed, Reason}}
    end.

handle_call({validate_key, Key}, _From, State) ->
    Result = do_validate_key(Key),
    {reply, Result, State};

handle_call({validate_key, Key, DbName}, _From, State) ->
    Result = do_validate_key(Key, DbName),
    {reply, Result, State};

handle_call({create_key, Opts}, _From, State) ->
    Result = do_create_key(Opts),
    {reply, Result, State};

handle_call({delete_key, KeyPrefix}, _From, State) ->
    Result = do_delete_key(KeyPrefix),
    {reply, Result, State};

handle_call(list_keys, _From, State) ->
    Result = do_list_keys(),
    {reply, Result, State};

handle_call(has_any_keys, _From, State) ->
    Result = dets:info(?TABLE, size) > 0,
    {reply, Result, State};

handle_call(generate_admin_key, _From, State) ->
    Result = do_generate_admin_key(),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    dets:close(?TABLE),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

maybe_bootstrap_admin_key(_State) ->
    %% Check for environment variable override
    case os:getenv("BARREL_DOCDB_ADMIN_KEY") of
        false ->
            %% No env var - check if any admin keys exist
            case has_admin_key() of
                true ->
                    ok;
                false ->
                    %% Auto-generate admin key
                    case do_generate_admin_key() of
                        {ok, Key} ->
                            logger:warning("========================================"),
                            logger:warning("No admin key configured. Auto-generated:"),
                            logger:warning("  ~s", [Key]),
                            logger:warning("Store this key securely!"),
                            logger:warning("It will not be shown again."),
                            logger:warning("Set BARREL_DOCDB_ADMIN_KEY env var to use a specific key."),
                            logger:warning("========================================"),
                            ok;
                        {error, Reason} ->
                            logger:error("Failed to generate admin key: ~p", [Reason]),
                            ok
                    end
            end;
        AdminKey ->
            %% Env var set - ensure it's stored as admin key
            KeyBin = list_to_binary(AdminKey),
            KeyHash = hash_key(KeyBin),
            case dets:lookup(?TABLE, KeyHash) of
                [] ->
                    %% Store the env var key as admin
                    KeyPrefix = key_prefix(KeyBin),
                    Record = #api_key{
                        key_hash = KeyHash,
                        key_prefix = KeyPrefix,
                        name = <<"Environment Admin Key">>,
                        permissions = [<<"read">>, <<"write">>, <<"admin">>],
                        databases = all,
                        is_admin = true,
                        created_at = erlang:system_time(millisecond),
                        last_used = undefined
                    },
                    ok = dets:insert(?TABLE, Record),
                    dets:sync(?TABLE),
                    logger:info("Admin key configured from environment variable"),
                    ok;
                [_] ->
                    %% Already exists
                    ok
            end
    end.

has_admin_key() ->
    dets:foldl(
        fun(#api_key{is_admin = true}, _Acc) -> true;
           (_, Acc) -> Acc
        end,
        false,
        ?TABLE
    ).

do_validate_key(Key) ->
    KeyHash = hash_key(Key),
    case dets:lookup(?TABLE, KeyHash) of
        [] ->
            {error, invalid_key};
        [Record] ->
            %% Update last_used timestamp
            UpdatedRecord = Record#api_key{last_used = erlang:system_time(millisecond)},
            dets:insert(?TABLE, UpdatedRecord),
            {ok, record_to_map(Record)}
    end.

do_validate_key(Key, DbName) ->
    KeyHash = hash_key(Key),
    case dets:lookup(?TABLE, KeyHash) of
        [] ->
            {error, invalid_key};
        [Record] ->
            %% Check database access
            case check_db_access(Record, DbName) of
                true ->
                    %% Update last_used timestamp
                    UpdatedRecord = Record#api_key{last_used = erlang:system_time(millisecond)},
                    dets:insert(?TABLE, UpdatedRecord),
                    {ok, record_to_map(Record)};
                false ->
                    {error, access_denied}
            end
    end.

%% Check if a key has access to a specific database
check_db_access(#api_key{is_admin = true}, _DbName) ->
    %% Admin keys have access to all databases
    true;
check_db_access(#api_key{databases = all}, _DbName) ->
    %% Global keys have access to all databases
    true;
check_db_access(#api_key{databases = Dbs}, DbName) when is_list(Dbs) ->
    lists:member(DbName, Dbs);
check_db_access(_, _) ->
    %% Legacy keys without databases field - treat as global
    true.

do_create_key(Opts) ->
    Name = maps:get(name, Opts, <<"unnamed">>),
    Permissions = maps:get(permissions, Opts, [<<"read">>]),
    Databases = maps:get(databases, Opts, all),
    IsAdmin = maps:get(is_admin, Opts, false),

    %% Generate new key
    Key = generate_key(),
    KeyHash = hash_key(Key),
    KeyPrefix = key_prefix(Key),

    Record = #api_key{
        key_hash = KeyHash,
        key_prefix = KeyPrefix,
        name = Name,
        permissions = Permissions,
        databases = Databases,
        is_admin = IsAdmin,
        created_at = erlang:system_time(millisecond),
        last_used = undefined
    },

    case dets:insert(?TABLE, Record) of
        ok ->
            dets:sync(?TABLE),
            {ok, Key, record_to_map(Record)};
        {error, Reason} ->
            {error, Reason}
    end.

do_delete_key(KeyPrefix) ->
    %% Find key by prefix
    case find_by_prefix(KeyPrefix) of
        {ok, #api_key{is_admin = true} = Record} ->
            %% Check if this is the last admin key
            AdminCount = count_admin_keys(),
            if
                AdminCount =< 1 ->
                    {error, cannot_delete_last_admin_key};
                true ->
                    dets:delete(?TABLE, Record#api_key.key_hash),
                    dets:sync(?TABLE),
                    ok
            end;
        {ok, Record} ->
            dets:delete(?TABLE, Record#api_key.key_hash),
            dets:sync(?TABLE),
            ok;
        {error, not_found} ->
            {error, not_found}
    end.

do_list_keys() ->
    Keys = dets:foldl(
        fun(Record, Acc) ->
            [record_to_map(Record) | Acc]
        end,
        [],
        ?TABLE
    ),
    {ok, lists:reverse(Keys)}.

do_generate_admin_key() ->
    Key = generate_key(),
    KeyHash = hash_key(Key),
    KeyPrefix = key_prefix(Key),

    Record = #api_key{
        key_hash = KeyHash,
        key_prefix = KeyPrefix,
        name = <<"Admin Key">>,
        permissions = [<<"read">>, <<"write">>, <<"admin">>],
        databases = all,
        is_admin = true,
        created_at = erlang:system_time(millisecond),
        last_used = undefined
    },

    case dets:insert(?TABLE, Record) of
        ok ->
            dets:sync(?TABLE),
            {ok, Key};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_prefix(Prefix) ->
    dets:foldl(
        fun(#api_key{key_prefix = P} = Record, _Acc) when P =:= Prefix ->
                {ok, Record};
           (_, Acc) ->
                Acc
        end,
        {error, not_found},
        ?TABLE
    ).

count_admin_keys() ->
    dets:foldl(
        fun(#api_key{is_admin = true}, Acc) -> Acc + 1;
           (_, Acc) -> Acc
        end,
        0,
        ?TABLE
    ).

%% Generate a new API key
generate_key() ->
    Random = crypto:strong_rand_bytes(24),
    Encoded = base64:encode(Random),
    %% Clean up base64 for URL safety
    Clean = binary:replace(binary:replace(Encoded, <<"+">>, <<"-">>, [global]),
                           <<"/">>, <<"_">>, [global]),
    <<"ak_", Clean/binary>>.

%% Hash a key for storage
hash_key(Key) ->
    crypto:hash(sha256, Key).

%% Get prefix for display (first 8 chars)
key_prefix(Key) ->
    binary:part(Key, 0, min(12, byte_size(Key))).

%% Convert record to map for API responses
record_to_map(#api_key{
    key_prefix = Prefix,
    name = Name,
    permissions = Permissions,
    databases = Databases,
    is_admin = IsAdmin,
    created_at = CreatedAt,
    last_used = LastUsed
}) ->
    #{
        key_prefix => Prefix,
        name => Name,
        permissions => Permissions,
        databases => Databases,
        is_admin => IsAdmin,
        created_at => CreatedAt,
        last_used => LastUsed
    }.
