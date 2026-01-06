%%%-------------------------------------------------------------------
%%% @doc Tiered Storage Manager for barrel_docdb
%%%
%%% Manages document lifecycle across storage tiers:
%%% - Hot: Recently accessed, high performance storage
%%% - Warm: Less frequently accessed, balanced storage
%%% - Cold: Archived data, optimized for space
%%%
%%% == Operation Modes ==
%%%
%%% **Metadata-only mode** (no warm_db/cold_db):
%%% Documents stay in the same database but are tagged with tier metadata.
%%% Useful for TTL management and tier-based queries without data movement.
%%%
%%% **Physical migration mode** (warm_db/cold_db configured):
%%% Documents physically move between separate databases. Required when
%%% using capacity-based auto-migration to actually free space.
%%%
%%% Configuration is stored per-database in a local document
%%% (`_local/_tier_config`) and cached in persistent_term for fast access.
%%%
%%% == Setup Examples ==
%%%
%%% Metadata-only (TTL and tier tagging):
%%%   barrel_tier:configure(<<"mydb">>, #{
%%%       enabled => true,
%%%       hot_threshold => 7 * 24 * 3600,    %% 7 days
%%%       warm_threshold => 30 * 24 * 3600   %% 30 days
%%%   }).
%%%
%%% Physical migration (capacity-based):
%%%   barrel_tier:configure(<<"mydb">>, #{
%%%       enabled => true,
%%%       auto_migrate => true,
%%%       capacity_limit => 10_000_000_000,  %% 10GB
%%%       warm_db => <<"mydb_warm">>,        %% Required for capacity migration
%%%       cold_db => <<"mydb_cold">>         %% Optional
%%%   }).
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_tier).

%% API
-export([
    %% Configuration (stored in local document, cached in persistent_term)
    configure/2,
    get_config/1,
    is_enabled/1,
    enable/1,
    disable/1,
    remove_config/1,
    load_config/1,

    %% TTL operations
    set_ttl/4,
    get_ttl/3,
    is_expired/3,

    %% Tier operations
    get_tier/3,
    set_tier/4,
    classify_by_age/3,

    %% Migration
    migrate_expired/2,
    migrate_to_tier/4,

    %% Capacity monitoring
    get_db_size/1,
    get_capacity_info/1,
    is_capacity_exceeded/1,
    check_capacity/1,

    %% Migration policy
    apply_migration_policy/1,
    run_migration/1,
    run_migration/2,

    %% Query across tiers
    find/3,
    fold_all_tiers/4,

    %% Tier value encoding
    tier_to_byte/1,
    byte_to_tier/1
]).

%% Tier constants
-define(TIER_HOT, 0).
-define(TIER_WARM, 1).
-define(TIER_COLD, 2).

%% Local document ID for tier config
-define(TIER_CONFIG_DOC_ID, <<"_tier_config">>).

%% Default thresholds (in seconds)
-define(DEFAULT_HOT_THRESHOLD, 7 * 24 * 3600).   %% 7 days
-define(DEFAULT_WARM_THRESHOLD, 30 * 24 * 3600). %% 30 days

-type tier() :: hot | warm | cold.
-type tier_config() :: #{
    enabled => boolean(),             %% Enable tiered storage strategy
    hot_threshold => pos_integer(),   %% Age in seconds for hot tier
    warm_threshold => pos_integer(),  %% Age in seconds for warm tier
    capacity_limit => pos_integer(),  %% Max bytes before migration
    auto_migrate => boolean(),        %% Enable automatic migration
    warm_db => binary(),              %% Optional: separate database for warm tier
    cold_db => binary()               %% Optional: separate database for cold tier
}.

-export_type([tier/0, tier_config/0]).

%%====================================================================
%% Configuration API
%%====================================================================

%% @doc Configure tier settings for a database
%% Stores config in local document and updates persistent_term cache.
%%
%% If capacity_limit is set with auto_migrate=true, at least warm_db must be
%% configured to have a destination for migrated documents.
-spec configure(binary(), tier_config()) -> ok | {error, term()}.
configure(DbName, Config) ->
    case validate_config(Config) of
        ok ->
            case barrel_docdb:db_pid(DbName) of
                {ok, Pid} ->
                    %% Merge with defaults to ensure all keys present
                    MergedConfig = maps:merge(default_config(), Config),
                    %% Store in local document
                    ok = barrel_db_server:put_local_doc(Pid, ?TIER_CONFIG_DOC_ID, MergedConfig),
                    %% Update persistent_term cache
                    persistent_term:put({barrel_tier_config, DbName}, MergedConfig),
                    ok;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Validate tier configuration
validate_config(Config) ->
    AutoMigrate = maps:get(auto_migrate, Config, false),
    CapacityLimit = maps:get(capacity_limit, Config, undefined),
    WarmDb = maps:get(warm_db, Config, undefined),

    %% If capacity-based auto-migration is configured, need a destination DB
    case AutoMigrate andalso is_integer(CapacityLimit) andalso CapacityLimit > 0 of
        true when WarmDb =:= undefined ->
            {error, {missing_config, warm_db,
                     <<"warm_db required when capacity_limit and auto_migrate are set">>}};
        _ ->
            ok
    end.

%% @doc Get tier configuration for a database (from cache or local doc)
-spec get_config(binary()) -> tier_config().
get_config(DbName) ->
    case persistent_term:get({barrel_tier_config, DbName}, undefined) of
        undefined ->
            %% Try loading from local document
            load_config(DbName);
        Config ->
            Config
    end.

%% @doc Check if tiered storage is enabled for a database
-spec is_enabled(binary()) -> boolean().
is_enabled(DbName) ->
    Config = get_config(DbName),
    maps:get(enabled, Config, false).

%% @doc Enable tiered storage for a database (with default settings)
-spec enable(binary()) -> ok | {error, term()}.
enable(DbName) ->
    Config = get_config(DbName),
    configure(DbName, Config#{enabled => true}).

%% @doc Disable tiered storage for a database
-spec disable(binary()) -> ok | {error, term()}.
disable(DbName) ->
    Config = get_config(DbName),
    configure(DbName, Config#{enabled => false}).

%% @doc Remove tier configuration completely for a database
%% This deletes the config document and clears the persistent_term cache.
%% Unlike disable/1, this removes all tier state, not just pauses it.
-spec remove_config(binary()) -> ok | {error, term()}.
remove_config(DbName) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            %% Delete local document (ignore not_found)
            _ = barrel_db_server:delete_local_doc(Pid, ?TIER_CONFIG_DOC_ID),
            %% Clear persistent_term cache
            persistent_term:erase({barrel_tier_config, DbName}),
            ok;
        {error, _} = Err ->
            Err
    end.

%% @doc Load config from local document into persistent_term cache
%% Called on database open or when cache is empty.
-spec load_config(binary()) -> tier_config().
load_config(DbName) ->
    Default = default_config(),
    Config = case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            case barrel_db_server:get_local_doc(Pid, ?TIER_CONFIG_DOC_ID) of
                {ok, Doc} ->
                    %% Merge with defaults to ensure all keys present
                    maps:merge(Default, Doc);
                {error, not_found} ->
                    Default
            end;
        {error, _} ->
            Default
    end,
    %% Cache in persistent_term
    persistent_term:put({barrel_tier_config, DbName}, Config),
    Config.

%% @private Default configuration
%% Note: enabled => true by default - tier features are active when configured.
%% Use disable/1 to temporarily pause, remove_config/1 to completely remove.
default_config() ->
    #{
        enabled => true,
        hot_threshold => ?DEFAULT_HOT_THRESHOLD,
        warm_threshold => ?DEFAULT_WARM_THRESHOLD,
        auto_migrate => false
    }.

%%====================================================================
%% TTL Operations
%%====================================================================

%% @doc Set TTL (time-to-live) for a document in seconds
%% The document will be marked for expiration after TTL seconds from now.
-spec set_ttl(pid(), binary(), pos_integer(), map()) -> ok | {error, term()}.
set_ttl(DbPid, DocId, TtlSeconds, Opts) ->
    ExpiresAt = erlang:system_time(millisecond) + (TtlSeconds * 1000),
    gen_server:call(DbPid, {set_doc_ttl, DocId, ExpiresAt, Opts}).

%% @doc Get the TTL info for a document
%% Returns {ok, #{expires_at => Timestamp, remaining => Seconds}} or {ok, undefined}
-spec get_ttl(pid(), binary(), map()) -> {ok, map() | undefined} | {error, term()}.
get_ttl(DbPid, DocId, Opts) ->
    gen_server:call(DbPid, {get_doc_ttl, DocId, Opts}).

%% @doc Check if a document has expired
-spec is_expired(pid(), binary(), map()) -> boolean().
is_expired(DbPid, DocId, Opts) ->
    case get_ttl(DbPid, DocId, Opts) of
        {ok, #{expires_at := ExpiresAt}} when ExpiresAt > 0 ->
            erlang:system_time(millisecond) >= ExpiresAt;
        _ ->
            false
    end.

%%====================================================================
%% Tier Operations
%%====================================================================

%% @doc Get the current tier of a document
-spec get_tier(pid(), binary(), map()) -> {ok, tier()} | {error, term()}.
get_tier(DbPid, DocId, Opts) ->
    gen_server:call(DbPid, {get_doc_tier, DocId, Opts}).

%% @doc Set the tier for a document
-spec set_tier(pid(), binary(), tier(), map()) -> ok | {error, term()}.
set_tier(DbPid, DocId, Tier, Opts) ->
    gen_server:call(DbPid, {set_doc_tier, DocId, Tier, Opts}).

%% @doc Classify documents by age into tiers
%% Scans documents and updates their tier classification based on age.
%% Returns #{classified => Count, errors => ErrorCount}
-spec classify_by_age(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
classify_by_age(DbPid, DbName, Opts) ->
    Config = get_config(DbName),
    HotThreshold = maps:get(hot_threshold, Config, ?DEFAULT_HOT_THRESHOLD),
    WarmThreshold = maps:get(warm_threshold, Config, ?DEFAULT_WARM_THRESHOLD),

    Now = erlang:system_time(millisecond),
    HotCutoff = Now - (HotThreshold * 1000),
    WarmCutoff = Now - (WarmThreshold * 1000),

    gen_server:call(DbPid, {classify_by_age, HotCutoff, WarmCutoff, Opts}, infinity).

%%====================================================================
%% Migration Operations
%%====================================================================

%% @doc Migrate expired documents
%% Either deletes them or moves to archive based on config.
%% Returns #{migrated => Count, deleted => Count}
-spec migrate_expired(pid(), map()) -> {ok, map()} | {error, term()}.
migrate_expired(DbPid, Opts) ->
    gen_server:call(DbPid, {migrate_expired, Opts}, infinity).

%% @doc Migrate documents to a specific tier
%% Filter specifies which documents to migrate (by age, path, etc.)
-spec migrate_to_tier(pid(), tier(), map(), map()) -> {ok, map()} | {error, term()}.
migrate_to_tier(DbPid, TargetTier, Filter, Opts) ->
    gen_server:call(DbPid, {migrate_to_tier, TargetTier, Filter, Opts}, infinity).

%%====================================================================
%% Capacity Monitoring
%%====================================================================

%% @doc Get the current database size in bytes
-spec get_db_size(binary()) -> {ok, non_neg_integer()} | {error, term()}.
get_db_size(DbName) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            case barrel_db_server:get_store_ref(Pid) of
                {ok, StoreRef} ->
                    barrel_store_rocksdb:get_db_size(StoreRef);
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Get capacity information for a database
%% Returns #{size => bytes, limit => bytes | undefined, used_percent => float()}
-spec get_capacity_info(binary()) -> {ok, map()} | {error, term()}.
get_capacity_info(DbName) ->
    case get_db_size(DbName) of
        {ok, Size} ->
            Config = get_config(DbName),
            Limit = maps:get(capacity_limit, Config, undefined),
            UsedPercent = case Limit of
                undefined -> 0.0;
                0 -> 0.0;
                _ -> (Size / Limit) * 100.0
            end,
            {ok, #{
                size => Size,
                limit => Limit,
                used_percent => UsedPercent
            }};
        {error, _} = Err ->
            Err
    end.

%% @doc Check if capacity threshold is exceeded
%% Returns true if capacity_limit is configured and current size exceeds it.
-spec is_capacity_exceeded(binary()) -> boolean().
is_capacity_exceeded(DbName) ->
    case get_capacity_info(DbName) of
        {ok, #{size := Size, limit := Limit}} when is_integer(Limit), Limit > 0 ->
            Size >= Limit;
        _ ->
            false
    end.

%% @doc Check capacity and return migration recommendation
%% Returns {ok, no_action} if capacity is fine, or
%% {ok, #{action => migrate, reason => capacity_exceeded, size => Size, limit => Limit}}
%% if migration is recommended.
-spec check_capacity(binary()) -> {ok, map()}.
check_capacity(DbName) ->
    case get_capacity_info(DbName) of
        {ok, #{size := Size, limit := Limit} = Info} when is_integer(Limit), Limit > 0 ->
            case Size >= Limit of
                true ->
                    {ok, #{
                        action => migrate,
                        reason => capacity_exceeded,
                        size => Size,
                        limit => Limit,
                        used_percent => maps:get(used_percent, Info, 0.0)
                    }};
                false ->
                    %% Check if approaching threshold (90%)
                    case (Size / Limit) >= 0.9 of
                        true ->
                            {ok, #{
                                action => warning,
                                reason => approaching_capacity,
                                size => Size,
                                limit => Limit,
                                used_percent => maps:get(used_percent, Info, 0.0)
                            }};
                        false ->
                            {ok, #{action => no_action}}
                    end
            end;
        {ok, _} ->
            %% No capacity limit configured
            {ok, #{action => no_action}};
        {error, Reason} ->
            {ok, #{action => error, reason => Reason}}
    end.

%%====================================================================
%% Migration Policy
%%====================================================================

%% @doc Apply migration policy for a database
%% Checks capacity and age thresholds, performs migration if needed.
%% Returns #{migrated => Count, action => atom(), ...}
-spec apply_migration_policy(binary()) -> {ok, map()} | {error, term()}.
apply_migration_policy(DbName) ->
    Config = get_config(DbName),
    case maps:get(auto_migrate, Config, false) of
        true ->
            run_migration(DbName, Config);
        false ->
            {ok, #{action => disabled, reason => auto_migrate_disabled}}
    end.

%% @doc Run migration for a database with default options
-spec run_migration(binary()) -> {ok, map()} | {error, term()}.
run_migration(DbName) ->
    Config = get_config(DbName),
    run_migration(DbName, Config).

%% @doc Run migration for a database with specified config
%% Performs migrations based on:
%% 1. Capacity - if exceeded, migrate oldest hot docs to warm
%% 2. Age - classify and migrate docs based on age thresholds
%% 3. Expired - delete/archive expired documents
-spec run_migration(binary(), tier_config()) -> {ok, map()} | {error, term()}.
run_migration(DbName, Config) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            Results = #{
                expired => #{deleted => 0},
                capacity => #{migrated => 0},
                age => #{classified => 0}
            },

            %% Step 1: Clean up expired documents
            ExpiredResult = case migrate_expired(Pid, #{}) of
                {ok, ExpStats} -> ExpStats;
                _ -> #{deleted => 0}
            end,

            %% Step 2: Check and handle capacity
            CapacityResult = case is_capacity_exceeded(DbName) of
                true ->
                    %% Migrate oldest hot docs to warm tier
                    case migrate_to_tier(Pid, warm, #{tier => hot}, #{}) of
                        {ok, CapStats} -> CapStats;
                        _ -> #{migrated => 0}
                    end;
                false ->
                    #{migrated => 0, action => no_action}
            end,

            %% Step 3: Classify documents by age
            AgeResult = case maps:get(enabled, Config, false) of
                true ->
                    case classify_by_age(Pid, DbName, #{}) of
                        {ok, AgeStats} -> AgeStats;
                        _ -> #{classified => 0}
                    end;
                false ->
                    #{classified => 0, action => tier_disabled}
            end,

            %% Aggregate results
            FinalResults = Results#{
                expired := ExpiredResult,
                capacity := CapacityResult,
                age := AgeResult,
                action => completed
            },

            {ok, FinalResults};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Cross-Tier Query
%%====================================================================

%% @doc Query across all tiers
%% Merges results from hot, warm, and cold storage.
-spec find(binary(), map(), map()) -> {ok, [map()], map()} | {error, term()}.
find(DbName, Query, Opts) ->
    Config = get_config(DbName),

    %% Determine which tiers to query based on options
    Tiers = maps:get(tiers, Opts, [hot, warm, cold]),

    %% Query each tier and merge results
    Results = lists:flatmap(
        fun(Tier) ->
            case query_tier(DbName, Tier, Query, Config) of
                {ok, TierResults, _Meta} -> TierResults;
                {error, _} -> []
            end
        end,
        Tiers
    ),

    %% Deduplicate by doc ID (newest revision wins)
    Merged = merge_results(Results),

    {ok, Merged, #{tiers => Tiers}}.

%% @doc Fold over documents across all tiers
-spec fold_all_tiers(binary(), fun((map(), term()) -> term()), term(), map()) -> term().
fold_all_tiers(DbName, Fun, Acc0, Opts) ->
    Tiers = maps:get(tiers, Opts, [hot, warm, cold]),

    lists:foldl(
        fun(Tier, Acc) ->
            fold_tier(DbName, Tier, Fun, Acc, Opts)
        end,
        Acc0,
        Tiers
    ).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Query a specific tier
query_tier(DbName, hot, Query, _Config) ->
    barrel_docdb:find(DbName, Query);
query_tier(DbName, warm, Query, Config) ->
    case maps:get(warm_db, Config, undefined) of
        undefined -> barrel_docdb:find(DbName, Query#{tier => warm});
        WarmDbName -> barrel_docdb:find(WarmDbName, Query)
    end;
query_tier(DbName, cold, Query, Config) ->
    case maps:get(cold_db, Config, undefined) of
        undefined -> barrel_docdb:find(DbName, Query#{tier => cold});
        ColdDbName -> barrel_docdb:find(ColdDbName, Query)
    end.

%% @private Fold over a specific tier
fold_tier(DbName, hot, Fun, Acc, Opts) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            barrel_db_server:fold_docs(Pid, Fun, Acc, Opts);
        _ ->
            Acc
    end;
fold_tier(DbName, Tier, Fun, Acc, Opts) ->
    Config = get_config(DbName),
    TierDbKey = case Tier of
        warm -> warm_db;
        cold -> cold_db
    end,
    case maps:get(TierDbKey, Config, undefined) of
        undefined -> Acc;
        TierDbName ->
            case barrel_docdb:db_pid(TierDbName) of
                {ok, Pid} -> barrel_db_server:fold_docs(Pid, Fun, Acc, Opts);
                _ -> Acc
            end
    end.

%% @private Merge results from multiple tiers, deduplicating by doc ID
merge_results(Results) ->
    Grouped = lists:foldl(
        fun(Doc, Acc) ->
            DocId = maps:get(<<"id">>, Doc, maps:get(id, Doc, undefined)),
            case DocId of
                undefined -> Acc;
                _ ->
                    Existing = maps:get(DocId, Acc, []),
                    Acc#{DocId => [Doc | Existing]}
            end
        end,
        #{},
        Results
    ),

    maps:fold(
        fun(_DocId, Docs, Acc) ->
            [newest_doc(Docs) | Acc]
        end,
        [],
        Grouped
    ).

%% @private Pick the newest document from a list
newest_doc([Doc]) -> Doc;
newest_doc(Docs) ->
    lists:foldl(
        fun(Doc, Best) ->
            DocRev = maps:get(<<"_rev">>, Doc, maps:get(rev, Doc, <<"0-">>)),
            BestRev = maps:get(<<"_rev">>, Best, maps:get(rev, Best, <<"0-">>)),
            case compare_revs(DocRev, BestRev) of
                gt -> Doc;
                _ -> Best
            end
        end,
        hd(Docs),
        tl(Docs)
    ).

%% @private Compare revision strings
compare_revs(Rev1, Rev2) ->
    Gen1 = parse_rev_gen(Rev1),
    Gen2 = parse_rev_gen(Rev2),
    if
        Gen1 > Gen2 -> gt;
        Gen1 < Gen2 -> lt;
        true -> eq
    end.

%% @private Parse revision generation number
parse_rev_gen(Rev) when is_binary(Rev) ->
    case binary:split(Rev, <<"-">>) of
        [GenBin | _] ->
            try binary_to_integer(GenBin)
            catch _:_ -> 0
            end;
        _ -> 0
    end.

%%====================================================================
%% Tier Value Encoding/Decoding
%%====================================================================

-spec tier_to_byte(tier()) -> 0..2.
tier_to_byte(hot) -> ?TIER_HOT;
tier_to_byte(warm) -> ?TIER_WARM;
tier_to_byte(cold) -> ?TIER_COLD.

-spec byte_to_tier(0..2) -> tier().
byte_to_tier(?TIER_HOT) -> hot;
byte_to_tier(?TIER_WARM) -> warm;
byte_to_tier(?TIER_COLD) -> cold.
