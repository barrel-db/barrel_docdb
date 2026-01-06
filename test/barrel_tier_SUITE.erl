%%%-------------------------------------------------------------------
%%% @doc Tiered Storage Test Suite
%%%
%%% Tests for barrel_tier module - TTL, tier operations, and configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_tier_SUITE).

-include_lib("common_test/include/ct.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    configure_tier/1,
    configure_validation/1,
    enable_disable_tier/1,
    remove_config/1,
    set_get_ttl/1,
    ttl_expiration/1,
    set_get_tier/1,
    classify_by_age/1,
    migrate_expired/1,
    migrate_to_tier/1,
    cross_tier_query/1,
    %% Capacity monitoring
    capacity_monitoring/1,
    capacity_exceeded/1,
    %% Migration policy
    migration_policy/1,
    auto_migration/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, tier_tests}].

groups() ->
    [{tier_tests, [sequence], [
        configure_tier,
        configure_validation,
        enable_disable_tier,
        remove_config,
        set_get_ttl,
        ttl_expiration,
        set_get_tier,
        classify_by_age,
        migrate_expired,
        migrate_to_tier,
        cross_tier_query,
        capacity_monitoring,
        capacity_exceeded,
        migration_policy,
        auto_migration
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(tier_tests, Config) ->
    %% Create test database
    {ok, _} = barrel_docdb:create_db(<<"tier_test_db">>),
    Config.

end_per_group(tier_tests, _Config) ->
    barrel_docdb:delete_db(<<"tier_test_db">>),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test tier configuration
configure_tier(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Configure tiered storage
    TierConfig = #{
        enabled => true,
        hot_threshold => 3600,      %% 1 hour
        warm_threshold => 86400,    %% 1 day
        auto_migrate => false
    },
    ok = barrel_tier:configure(DbName, TierConfig),

    %% Verify config is stored and cached
    Config = barrel_tier:get_config(DbName),
    true = maps:get(enabled, Config),
    3600 = maps:get(hot_threshold, Config),
    86400 = maps:get(warm_threshold, Config),

    ok.

%% @doc Test configuration validation
configure_validation(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Metadata-only mode (no warm_db) is OK without capacity_limit
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => true
        %% No capacity_limit, so warm_db not required
    }),

    %% Capacity-based migration without warm_db should fail
    {error, {missing_config, warm_db, _}} = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => true,
        capacity_limit => 1000000  %% Capacity set, but no warm_db
    }),

    %% Capacity-based migration WITH warm_db should succeed
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => true,
        capacity_limit => 1000000,
        warm_db => <<"tier_test_db_warm">>  %% Destination DB configured
    }),

    %% Reset to simple config for other tests
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => false
    }),

    ok.

%% @doc Test enable/disable tiered storage
enable_disable_tier(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Disable first
    ok = barrel_tier:disable(DbName),
    false = barrel_tier:is_enabled(DbName),

    %% Enable
    ok = barrel_tier:enable(DbName),
    true = barrel_tier:is_enabled(DbName),

    %% Disable again
    ok = barrel_tier:disable(DbName),
    false = barrel_tier:is_enabled(DbName),

    ok.

%% @doc Test remove_config - complete removal of tier config
remove_config(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Configure tier
    ok = barrel_tier:configure(DbName, #{
        auto_migrate => false,
        hot_threshold => 3600
    }),

    %% Verify config is stored
    Config1 = barrel_tier:get_config(DbName),
    true = maps:get(enabled, Config1),  %% enabled => true is default
    3600 = maps:get(hot_threshold, Config1),

    %% Remove config completely
    ok = barrel_tier:remove_config(DbName),

    %% After removal, get_config returns default config (not the stored one)
    Config2 = barrel_tier:get_config(DbName),
    true = maps:get(enabled, Config2),  %% Default is true
    %% hot_threshold should be default (7 days = 604800 seconds)
    604800 = maps:get(hot_threshold, Config2),

    %% Re-configure for other tests
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => false
    }),

    ok.

%% @doc Test setting and getting TTL
set_get_ttl(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create a document
    Doc = #{<<"id">> => <<"ttl_doc">>, <<"value">> => 1},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Initially no TTL
    {ok, undefined} = barrel_tier:get_ttl(Pid, <<"ttl_doc">>, #{}),

    %% Set TTL (60 seconds)
    ok = barrel_tier:set_ttl(Pid, <<"ttl_doc">>, 60, #{}),

    %% Get TTL
    {ok, TtlInfo} = barrel_tier:get_ttl(Pid, <<"ttl_doc">>, #{}),
    true = is_map(TtlInfo),
    true = maps:is_key(expires_at, TtlInfo),
    true = maps:is_key(remaining, TtlInfo),

    %% Remaining should be close to 60
    Remaining = maps:get(remaining, TtlInfo),
    true = Remaining > 55 andalso Remaining =< 60,

    ok.

%% @doc Test TTL expiration check
ttl_expiration(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create a document
    Doc = #{<<"id">> => <<"expire_doc">>, <<"value">> => 1},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Not expired initially
    false = barrel_tier:is_expired(Pid, <<"expire_doc">>, #{}),

    %% Set TTL in the past (already expired)
    %% Note: set_ttl takes seconds, but stores as absolute milliseconds
    %% We need to set expires_at directly in the past
    Now = erlang:system_time(millisecond),
    PastTime = Now - 1000,  %% 1 second ago
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    DocEntityKey = barrel_store_keys:doc_entity(DbName, <<"expire_doc">>),
    {ok, Columns} = barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey),
    UpdatedColumns = lists:keyreplace(<<"expires_at">>, 1, Columns, {<<"expires_at">>, PastTime}),
    ok = barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, DocEntityKey, UpdatedColumns}]),

    %% Now should be expired
    true = barrel_tier:is_expired(Pid, <<"expire_doc">>, #{}),

    ok.

%% @doc Test setting and getting document tier
set_get_tier(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create a document
    Doc = #{<<"id">> => <<"tier_doc">>, <<"value">> => 1},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Initially hot (tier 0)
    {ok, hot} = barrel_tier:get_tier(Pid, <<"tier_doc">>, #{}),

    %% Set to warm
    ok = barrel_tier:set_tier(Pid, <<"tier_doc">>, warm, #{}),
    {ok, warm} = barrel_tier:get_tier(Pid, <<"tier_doc">>, #{}),

    %% Set to cold
    ok = barrel_tier:set_tier(Pid, <<"tier_doc">>, cold, #{}),
    {ok, cold} = barrel_tier:get_tier(Pid, <<"tier_doc">>, #{}),

    %% Set back to hot
    ok = barrel_tier:set_tier(Pid, <<"tier_doc">>, hot, #{}),
    {ok, hot} = barrel_tier:get_tier(Pid, <<"tier_doc">>, #{}),

    ok.

%% @doc Test classification by age
classify_by_age(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create documents
    lists:foreach(
        fun(N) ->
            Doc = #{<<"id">> => <<"classify_", (integer_to_binary(N))/binary>>, <<"n">> => N},
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        lists:seq(1, 5)
    ),

    %% Configure tier with thresholds
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        hot_threshold => 3600,
        warm_threshold => 86400
    }),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Classify - all should be hot since they're new
    {ok, Stats} = barrel_tier:classify_by_age(Pid, DbName, #{}),
    true = is_map(Stats),
    true = maps:is_key(classified, Stats),

    ok.

%% @doc Test migration of expired documents
migrate_expired(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create a document
    Doc = #{<<"id">> => <<"migrate_expired_doc">>, <<"value">> => 1},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% First migrate_expired call may cleanup stale docs from earlier tests
    %% Just run it to clean up state
    {ok, _Stats1} = barrel_tier:migrate_expired(Pid, #{}),

    %% Verify our doc is not expired yet
    false = barrel_tier:is_expired(Pid, <<"migrate_expired_doc">>, #{}),

    %% Set document as expired
    Now = erlang:system_time(millisecond),
    PastTime = Now - 1000,
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    DocEntityKey = barrel_store_keys:doc_entity(DbName, <<"migrate_expired_doc">>),
    {ok, Columns} = barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey),
    UpdatedColumns = lists:keyreplace(<<"expires_at">>, 1, Columns, {<<"expires_at">>, PastTime}),
    ok = barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, DocEntityKey, UpdatedColumns}]),

    %% Verify doc is now expired
    true = barrel_tier:is_expired(Pid, <<"migrate_expired_doc">>, #{}),

    %% Migrate expired - should delete the expired doc
    {ok, Stats2} = barrel_tier:migrate_expired(Pid, #{}),
    true = maps:get(deleted, Stats2) >= 1,

    ok.

%% @doc Test migration to specific tier
migrate_to_tier(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create documents
    lists:foreach(
        fun(N) ->
            Doc = #{<<"id">> => <<"migrate_tier_", (integer_to_binary(N))/binary>>, <<"n">> => N},
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        lists:seq(1, 3)
    ),

    %% Get db pid
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Migrate all hot docs to warm
    {ok, Stats} = barrel_tier:migrate_to_tier(Pid, warm, #{tier => hot}, #{}),
    true = is_map(Stats),
    Migrated = maps:get(migrated, Stats),
    true = Migrated >= 3,

    %% Verify docs are now warm
    {ok, warm} = barrel_tier:get_tier(Pid, <<"migrate_tier_1">>, #{}),
    {ok, warm} = barrel_tier:get_tier(Pid, <<"migrate_tier_2">>, #{}),

    ok.

%% @doc Test cross-tier query (uses barrel_tier:find/3)
cross_tier_query(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Create documents in different tiers
    {ok, Pid} = barrel_docdb:db_pid(DbName),

    %% Create hot doc
    Doc1 = #{<<"id">> => <<"cross_hot">>, <<"type">> => <<"test">>},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc1),

    %% Create warm doc
    Doc2 = #{<<"id">> => <<"cross_warm">>, <<"type">> => <<"test">>},
    {ok, _} = barrel_docdb:put_doc(DbName, Doc2),
    ok = barrel_tier:set_tier(Pid, <<"cross_warm">>, warm, #{}),

    %% Query across tiers
    Query = #{},
    {ok, _Results, Meta} = barrel_tier:find(DbName, Query, #{tiers => [hot, warm]}),

    %% Verify meta shows which tiers were queried
    [hot, warm] = maps:get(tiers, Meta),

    ok.

%% @doc Test capacity monitoring functions
capacity_monitoring(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Get database size
    {ok, Size} = barrel_tier:get_db_size(DbName),
    true = is_integer(Size),
    true = Size >= 0,

    %% Get capacity info (no limit configured yet)
    {ok, Info} = barrel_tier:get_capacity_info(DbName),
    true = is_map(Info),
    true = maps:is_key(size, Info),
    undefined = maps:get(limit, Info),
    +0.0 = maps:get(used_percent, Info),

    %% Not exceeded when no limit configured
    false = barrel_tier:is_capacity_exceeded(DbName),

    %% Check capacity returns no_action when no limit
    {ok, #{action := no_action}} = barrel_tier:check_capacity(DbName),

    ok.

%% @doc Test capacity exceeded detection
capacity_exceeded(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Add some documents to ensure DB has measurable size
    lists:foreach(
        fun(N) ->
            Doc = #{<<"id">> => <<"capacity_doc_", (integer_to_binary(N))/binary>>,
                    <<"data">> => <<"some data to take up space">>},
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        lists:seq(1, 10)
    ),

    %% Get current size
    {ok, CurrentSize} = barrel_tier:get_db_size(DbName),

    %% Configure with a very low capacity limit (set to 1 byte to ensure exceeded)
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        capacity_limit => 1  %% Set limit to 1 byte (will be exceeded)
    }),

    %% Should now be exceeded (unless size is 0)
    case CurrentSize > 0 of
        true ->
            true = barrel_tier:is_capacity_exceeded(DbName),

            %% Check capacity should recommend migration
            {ok, #{action := migrate, reason := capacity_exceeded}} = barrel_tier:check_capacity(DbName),

            %% Get capacity info should show > 100% usage
            {ok, Info} = barrel_tier:get_capacity_info(DbName),
            UsedPercent = maps:get(used_percent, Info),
            true = UsedPercent > 100.0;
        false ->
            %% Size is 0 (estimate not available), skip exceeded tests
            ct:log("Skipping exceeded checks - DB size is 0"),
            ok
    end,

    %% Reset config to remove limit
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        capacity_limit => undefined
    }),

    %% Should no longer be exceeded
    false = barrel_tier:is_capacity_exceeded(DbName),

    ok.

%% @doc Test migration policy - manual run
migration_policy(_Config) ->
    DbName = <<"tier_test_db">>,

    %% Run migration manually (with default config)
    {ok, Result} = barrel_tier:run_migration(DbName),
    true = is_map(Result),
    true = maps:is_key(expired, Result),
    true = maps:is_key(capacity, Result),
    true = maps:is_key(age, Result),

    %% Verify structure of results
    ExpiredStats = maps:get(expired, Result),
    true = is_map(ExpiredStats),

    CapacityStats = maps:get(capacity, Result),
    true = is_map(CapacityStats),

    AgeStats = maps:get(age, Result),
    true = is_map(AgeStats),

    ok.

%% @doc Test auto-migration policy
auto_migration(_Config) ->
    DbName = <<"tier_test_db">>,

    %% By default auto_migrate is false
    {ok, #{action := disabled}} = barrel_tier:apply_migration_policy(DbName),

    %% Enable auto_migrate
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => true
    }),

    %% Now apply_migration_policy should run
    {ok, Result} = barrel_tier:apply_migration_policy(DbName),
    completed = maps:get(action, Result),

    %% Verify it performed the migration steps
    true = maps:is_key(expired, Result),
    true = maps:is_key(capacity, Result),
    true = maps:is_key(age, Result),

    %% Disable auto_migrate
    ok = barrel_tier:configure(DbName, #{
        enabled => true,
        auto_migrate => false
    }),

    %% Should return disabled again
    {ok, #{action := disabled}} = barrel_tier:apply_migration_policy(DbName),

    ok.
