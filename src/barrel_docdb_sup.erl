%%%-------------------------------------------------------------------
%%% @doc barrel_docdb top-level supervisor
%%%
%%% Supervises all barrel_docdb processes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the barrel_docdb supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @doc Initialize the supervisor with child specs
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    %% Prometheus metrics module
    %% Must start first to set up metric declarations
    Metrics = #{
        id => barrel_metrics,
        start => {barrel_metrics, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_metrics]
    },

    %% Shared RocksDB block cache for all databases
    %% Must start before any database opens
    Cache = #{
        id => barrel_cache,
        start => {barrel_cache, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_cache]
    },

    %% Global HLC clock for distributed time synchronization
    %% Registered as 'barrel_hlc_clock' for node-wide access
    HlcMaxOffset = application:get_env(barrel_docdb, hlc_max_offset, 0),
    Hlc = #{
        id => barrel_hlc_clock,
        start => {hlc, start_link, [barrel_hlc_clock, fun hlc:physical_clock/0, HlcMaxOffset]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [hlc]
    },

    %% Subscription manager for path-based document subscriptions
    Sub = #{
        id => barrel_sub,
        start => {barrel_sub, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_sub]
    },

    %% Query subscription manager for query-based document subscriptions
    QuerySub = #{
        id => barrel_query_sub,
        start => {barrel_query_sub, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_query_sub]
    },

    %% Path dictionary for path ID interning (posting lists)
    PathDict = #{
        id => barrel_path_dict,
        start => {barrel_path_dict, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_path_dict]
    },

    %% Query cursor manager for chunked query execution
    QueryCursor = #{
        id => barrel_query_cursor,
        start => {barrel_query_cursor, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_query_cursor]
    },

    %% Parallel worker pool for query processing
    Parallel = #{
        id => barrel_parallel,
        start => {barrel_parallel, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_parallel]
    },

    %% Database supervisor for managing individual database processes
    %% Note: Each database starts its own compaction filter handler in barrel_db_server
    DbSup = #{
        id => barrel_db_sup,
        start => {barrel_db_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_db_sup]
    },

    %% VDB (Virtual Database) supervisor for sharded databases
    %% Manages VDB registry and future shard-related processes
    VdbSup = #{
        id => barrel_vdb_sup,
        start => {barrel_vdb_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_vdb_sup]
    },

    %% Replication task manager for persistent replication tasks
    RepTasks = #{
        id => barrel_rep_tasks,
        start => {barrel_rep_tasks, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_rep_tasks]
    },

    %% Replication policy manager for high-level replication patterns
    RepPolicy = #{
        id => barrel_rep_policy,
        start => {barrel_rep_policy, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_rep_policy]
    },

    %% API keys manager for HTTP authentication
    ApiKeys = #{
        id => barrel_http_api_keys,
        start => {barrel_http_api_keys, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_http_api_keys]
    },

    %% Discovery service for peer-to-peer federation
    %% Provides node info endpoint (/.well-known/barrel) and peer gossip
    Discovery = #{
        id => barrel_discovery,
        start => {barrel_discovery, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_discovery]
    },

    %% HTTP server for REST API (optional, controlled by http_enabled config)
    HttpEnabled = application:get_env(barrel_docdb, http_enabled, false),
    HttpOpts = build_http_opts(),
    HttpServer = #{
        id => barrel_http_server,
        start => {barrel_http_server, start_link, [HttpOpts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_http_server]
    },

    %% Base child specs (always started)
    BaseSpecs = [Metrics, Cache, Hlc, Sub, QuerySub, PathDict, QueryCursor, Parallel, DbSup, VdbSup, RepTasks, RepPolicy, ApiKeys, Discovery],

    %% Conditionally add HTTP server
    ChildSpecs = case HttpEnabled of
        true -> BaseSpecs ++ [HttpServer];
        false -> BaseSpecs
    end,

    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
%% Build HTTP server options from application environment
build_http_opts() ->
    %% Basic options
    Opts0 = #{
        port => application:get_env(barrel_docdb, http_port, 8080),
        num_acceptors => application:get_env(barrel_docdb, http_acceptors, 100),
        max_connections => application:get_env(barrel_docdb, http_max_connections, infinity),
        protocols => application:get_env(barrel_docdb, http_protocols, [http2, http])
    },

    %% TLS options (only add if TLS is enabled and cert/key are provided)
    TlsEnabled = application:get_env(barrel_docdb, http_tls_enabled, false),
    CertFile = application:get_env(barrel_docdb, http_certfile, undefined),
    KeyFile = application:get_env(barrel_docdb, http_keyfile, undefined),

    Opts1 = case TlsEnabled andalso is_valid_path(CertFile) andalso is_valid_path(KeyFile) of
        true ->
            TlsOpts = #{
                certfile => CertFile,
                keyfile => KeyFile
            },
            %% Add optional CA file
            TlsOpts1 = case application:get_env(barrel_docdb, http_cacertfile, undefined) of
                undefined -> TlsOpts;
                "" -> TlsOpts;
                CaFile -> TlsOpts#{cacertfile => CaFile}
            end,
            %% Add verify mode
            TlsOpts2 = case application:get_env(barrel_docdb, http_verify, verify_none) of
                verify_peer -> TlsOpts1#{verify => verify_peer};
                _ -> TlsOpts1#{verify => verify_none}
            end,
            maps:merge(Opts0, TlsOpts2);
        false ->
            Opts0
    end,
    Opts1.

%% @private
%% Check if path is valid (non-empty string/binary)
is_valid_path(undefined) -> false;
is_valid_path("") -> false;
is_valid_path(<<>>) -> false;
is_valid_path(Path) when is_list(Path); is_binary(Path) -> true;
is_valid_path(_) -> false.
