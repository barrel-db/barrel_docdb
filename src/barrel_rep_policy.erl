%%%-------------------------------------------------------------------
%%% @doc Replication Policy Manager for barrel_docdb
%%%
%%% Provides high-level replication patterns that compose the basic
%%% replication primitives into useful topologies:
%%%
%%% - **Chain**: A→B→C with confirmation propagation
%%% - **Tiered**: Hot→Warm→Cold automatic data migration
%%% - **Group**: Multi-master bidirectional replication
%%% - **Fanout**: One-to-many distribution
%%%
%%% Policies are stored persistently and automatically applied when
%%% the node starts.
%%%
%%% == Example: Chain Replication ==
%%% ```
%%% barrel_rep_policy:create(<<"my_chain">>, #{
%%%     pattern => chain,
%%%     nodes => [<<"node_a">>, <<"node_b">>, <<"node_c">>],
%%%     database => <<"mydb">>
%%% }).
%%% '''
%%%
%%% == Example: Group Replication ==
%%% ```
%%% barrel_rep_policy:create(<<"region_sync">>, #{
%%%     pattern => group,
%%%     members => [
%%%         <<"http://node1:8080/users">>,
%%%         <<"http://node2:8080/users">>,
%%%         <<"users">>  %% local
%%%     ]
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_policy).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    create/2,
    delete/1,
    get/1,
    list/0,
    enable/1,
    disable/1,
    status/1,
    apply_policy/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(POLICY_PREFIX, <<"_rep_policy:">>).

-record(state, {
    policies = #{} :: #{binary() => policy()},
    tasks = #{} :: #{binary() => [pid()]}  % policy_name => replication task pids
}).

-type pattern() :: chain | tiered | group | fanout.

-type policy() :: #{
    name := binary(),
    pattern := pattern(),
    enabled := boolean(),
    %% Chain pattern options
    nodes => [binary()],              % List of node URLs
    database => binary(),             % Database to replicate
    %% Group pattern options
    members => [binary()],            % List of db URLs (local or remote)
    %% Fanout pattern options
    source => binary(),               % Source database
    targets => [binary()],            % Target databases/URLs
    %% Tiered pattern options
    hot_db => binary(),               % Hot tier database
    warm_db => binary(),              % Warm tier database
    cold_db => binary(),              % Cold tier database
    %% Common options
    mode => continuous | one_shot,
    filter => map(),                  % Optional replication filter
    %% Authentication for remote nodes
    auth => map()                     % Auth config: #{bearer_token => Token} or #{basic_auth => {User, Pass}}
}.

-export_type([policy/0, pattern/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the replication policy manager
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Create a new replication policy
%%
%% == Patterns ==
%%
%% === Chain ===
%% Replicates A→B→C. Write confirmations propagate through the chain.
%% ```
%% #{pattern => chain,
%%%   nodes => [<<"http://a:8080">>, <<"http://b:8080">>, <<"http://c:8080">>],
%%%   database => <<"mydb">>}
%% '''
%%
%% === Group ===
%% Bidirectional replication between all members.
%% ```
%% #{pattern => group,
%%%   members => [<<"db1">>, <<"http://remote:8080/db1">>]}
%% '''
%%
%% === Fanout ===
%% One source replicates to multiple targets.
%% ```
%% #{pattern => fanout,
%%%   source => <<"master">>,
%%%   targets => [<<"replica1">>, <<"replica2">>]}
%% '''
%%
%% === Tiered ===
%% Uses barrel_tier for automatic data migration.
%% ```
%% #{pattern => tiered,
%%%   hot_db => <<"cache">>,
%%%   warm_db => <<"main">>,
%%%   cold_db => <<"archive">>}
%% '''
-spec create(binary(), map()) -> ok | {error, term()}.
create(Name, Config) ->
    gen_server:call(?SERVER, {create, Name, Config}).

%% @doc Delete a policy by name
-spec delete(binary()) -> ok | {error, not_found}.
delete(Name) ->
    gen_server:call(?SERVER, {delete, Name}).

%% @doc Get a policy by name
-spec get(binary()) -> {ok, policy()} | {error, not_found}.
get(Name) ->
    gen_server:call(?SERVER, {get, Name}).

%% @doc List all policies
-spec list() -> {ok, [policy()]}.
list() ->
    gen_server:call(?SERVER, list).

%% @doc Enable a policy (starts replication tasks)
-spec enable(binary()) -> ok | {error, term()}.
enable(Name) ->
    gen_server:call(?SERVER, {enable, Name}).

%% @doc Disable a policy (stops replication tasks)
-spec disable(binary()) -> ok | {error, term()}.
disable(Name) ->
    gen_server:call(?SERVER, {disable, Name}).

%% @doc Get status of a policy's replication tasks
-spec status(binary()) -> {ok, map()} | {error, not_found}.
status(Name) ->
    gen_server:call(?SERVER, {status, Name}).

%% @doc Apply a policy (set up replication based on pattern)
%% This is called internally when a policy is enabled.
-spec apply_policy(policy()) -> {ok, [pid()]} | {error, term()}.
apply_policy(Policy) ->
    Pattern = maps:get(pattern, Policy),
    apply_pattern(Pattern, Policy).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Load policies from persistent storage
    Policies = load_policies(),
    %% Auto-enable policies marked as enabled
    State = #state{policies = Policies},
    self() ! auto_enable,
    {ok, State}.

handle_call({create, Name, Config}, _From, State) ->
    case create_policy(Name, Config, State) of
        {ok, Policy, NewState} ->
            %% Persist the policy
            ok = save_policy(Policy),
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({delete, Name}, _From, State) ->
    case maps:find(Name, State#state.policies) of
        {ok, _Policy} ->
            %% Stop any running tasks
            NewState = stop_policy_tasks(Name, State),
            %% Remove from storage
            ok = delete_policy_storage(Name),
            %% Remove from state
            FinalState = NewState#state{
                policies = maps:remove(Name, NewState#state.policies)
            },
            {reply, ok, FinalState};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({get, Name}, _From, State) ->
    case maps:find(Name, State#state.policies) of
        {ok, Policy} ->
            {reply, {ok, Policy}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(list, _From, State) ->
    Policies = maps:values(State#state.policies),
    {reply, {ok, Policies}, State};

handle_call({enable, Name}, _From, State) ->
    case maps:find(Name, State#state.policies) of
        {ok, Policy} ->
            Policy2 = Policy#{enabled => true},
            case apply_policy(Policy2) of
                {ok, TaskPids} ->
                    ok = save_policy(Policy2),
                    NewState = State#state{
                        policies = maps:put(Name, Policy2, State#state.policies),
                        tasks = maps:put(Name, TaskPids, State#state.tasks)
                    },
                    {reply, ok, NewState};
                {error, _} = Error ->
                    {reply, Error, State}
            end;
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({disable, Name}, _From, State) ->
    case maps:find(Name, State#state.policies) of
        {ok, Policy} ->
            Policy2 = Policy#{enabled => false},
            ok = save_policy(Policy2),
            NewState = stop_policy_tasks(Name, State),
            FinalState = NewState#state{
                policies = maps:put(Name, Policy2, NewState#state.policies)
            },
            {reply, ok, FinalState};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call({status, Name}, _From, State) ->
    case maps:find(Name, State#state.policies) of
        {ok, Policy} ->
            TaskPids = maps:get(Name, State#state.tasks, []),
            Status = #{
                name => Name,
                pattern => maps:get(pattern, Policy),
                enabled => maps:get(enabled, Policy),
                task_count => length(TaskPids),
                tasks => [task_status(Pid) || Pid <- TaskPids]
            },
            {reply, {ok, Status}, State};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(auto_enable, State) ->
    %% Enable all policies marked as enabled
    NewState = lists:foldl(
        fun({Name, Policy}, AccState) ->
            case maps:get(enabled, Policy, false) of
                true ->
                    case apply_policy(Policy) of
                        {ok, TaskPids} ->
                            AccState#state{
                                tasks = maps:put(Name, TaskPids, AccState#state.tasks)
                            };
                        {error, Reason} ->
                            logger:warning("Failed to enable policy ~s: ~p", [Name, Reason]),
                            AccState
                    end;
                false ->
                    AccState
            end
        end,
        State,
        maps:to_list(State#state.policies)
    ),
    {noreply, NewState};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    %% A replication task died - remove from tracking
    %% In production, we might want to restart it
    NewTasks = maps:map(
        fun(_Name, Pids) ->
            lists:delete(Pid, Pids)
        end,
        State#state.tasks
    ),
    {noreply, State#state{tasks = NewTasks}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Stop all replication tasks
    lists:foreach(
        fun({Name, _Policy}) ->
            stop_policy_tasks(Name, State)
        end,
        maps:to_list(State#state.policies)
    ),
    ok.

%%====================================================================
%% Pattern Implementation
%%====================================================================

%% @private Apply replication based on pattern type
apply_pattern(chain, Policy) ->
    apply_chain_pattern(Policy);
apply_pattern(group, Policy) ->
    apply_group_pattern(Policy);
apply_pattern(fanout, Policy) ->
    apply_fanout_pattern(Policy);
apply_pattern(tiered, Policy) ->
    apply_tiered_pattern(Policy);
apply_pattern(Unknown, _Policy) ->
    {error, {unknown_pattern, Unknown}}.

%% @private Chain replication: A→B→C
%% Creates replications from each node to the next in the chain
apply_chain_pattern(Policy) ->
    Nodes = maps:get(nodes, Policy, []),
    Database = maps:get(database, Policy),
    Mode = maps:get(mode, Policy, continuous),
    Filter = maps:get(filter, Policy, #{}),
    Auth = maps:get(auth, Policy, #{}),

    %% Create pairs: [{A,B}, {B,C}]
    Pairs = chain_pairs(Nodes),

    %% Start a replication task for each pair
    Results = lists:map(
        fun({Source, Target}) ->
            start_replication_task(#{
                source => make_db_url(Source, Database),
                target => make_db_url(Target, Database),
                mode => Mode,
                filter => Filter,
                auth => Auth
            })
        end,
        Pairs
    ),

    collect_task_results(Results).

%% @private Group replication: All members replicate bidirectionally
apply_group_pattern(Policy) ->
    Members = maps:get(members, Policy, []),
    Mode = maps:get(mode, Policy, continuous),
    Filter = maps:get(filter, Policy, #{}),
    Auth = maps:get(auth, Policy, #{}),

    %% Create all pairs (bidirectional)
    Pairs = group_pairs(Members),

    Results = lists:map(
        fun({Source, Target}) ->
            start_replication_task(#{
                source => Source,
                target => Target,
                mode => Mode,
                filter => Filter,
                auth => Auth
            })
        end,
        Pairs
    ),

    collect_task_results(Results).

%% @private Fanout replication: Source→[Targets]
apply_fanout_pattern(Policy) ->
    Source = maps:get(source, Policy),
    Targets = maps:get(targets, Policy, []),
    Mode = maps:get(mode, Policy, continuous),
    Filter = maps:get(filter, Policy, #{}),
    Auth = maps:get(auth, Policy, #{}),

    Results = lists:map(
        fun(Target) ->
            start_replication_task(#{
                source => Source,
                target => Target,
                mode => Mode,
                filter => Filter,
                auth => Auth
            })
        end,
        Targets
    ),

    collect_task_results(Results).

%% @private Tiered replication: Use barrel_tier for data migration
%% This pattern configures barrel_tier rather than direct replication
apply_tiered_pattern(Policy) ->
    HotDb = maps:get(hot_db, Policy),
    WarmDb = maps:get(warm_db, Policy, undefined),
    ColdDb = maps:get(cold_db, Policy, undefined),

    %% Configure tier for hot database
    TierConfig = #{
        enabled => true
    },
    TierConfig2 = case WarmDb of
        undefined -> TierConfig;
        _ -> TierConfig#{warm_db => WarmDb}
    end,
    TierConfig3 = case ColdDb of
        undefined -> TierConfig2;
        _ -> TierConfig2#{cold_db => ColdDb}
    end,

    case barrel_tier:configure(HotDb, TierConfig3) of
        ok -> {ok, []};  % No direct replication tasks for tiered
        Error -> Error
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Create policy from config
create_policy(Name, Config, State) ->
    case maps:is_key(Name, State#state.policies) of
        true ->
            {error, already_exists};
        false ->
            Pattern = maps:get(pattern, Config),
            case validate_pattern_config(Pattern, Config) of
                ok ->
                    Policy = Config#{
                        name => Name,
                        enabled => to_boolean(maps:get(enabled, Config, false))
                    },
                    NewPolicies = maps:put(Name, Policy, State#state.policies),
                    {ok, Policy, State#state{policies = NewPolicies}};
                {error, _} = Error ->
                    Error
            end
    end.

%% @private Validate pattern-specific configuration
validate_pattern_config(chain, Config) ->
    case {maps:is_key(nodes, Config), maps:is_key(database, Config)} of
        {true, true} ->
            Nodes = maps:get(nodes, Config),
            case length(Nodes) >= 2 of
                true -> ok;
                false -> {error, {invalid_config, chain_requires_at_least_2_nodes}}
            end;
        _ ->
            {error, {invalid_config, chain_requires_nodes_and_database}}
    end;
validate_pattern_config(group, Config) ->
    case maps:is_key(members, Config) of
        true ->
            Members = maps:get(members, Config),
            case length(Members) >= 2 of
                true -> ok;
                false -> {error, {invalid_config, group_requires_at_least_2_members}}
            end;
        false ->
            {error, {invalid_config, group_requires_members}}
    end;
validate_pattern_config(fanout, Config) ->
    case {maps:is_key(source, Config), maps:is_key(targets, Config)} of
        {true, true} -> ok;
        _ -> {error, {invalid_config, fanout_requires_source_and_targets}}
    end;
validate_pattern_config(tiered, Config) ->
    case maps:is_key(hot_db, Config) of
        true -> ok;
        false -> {error, {invalid_config, tiered_requires_hot_db}}
    end;
validate_pattern_config(Unknown, _Config) ->
    {error, {unknown_pattern, Unknown}}.

%% @private Create chain pairs from node list
%% [A, B, C] -> [{A, B}, {B, C}]
chain_pairs([]) -> [];
chain_pairs([_]) -> [];
chain_pairs([A, B | Rest]) ->
    [{A, B} | chain_pairs([B | Rest])].

%% @private Create bidirectional pairs for group
%% [A, B, C] -> [{A,B}, {B,A}, {A,C}, {C,A}, {B,C}, {C,B}]
group_pairs(Members) ->
    [{A, B} || A <- Members, B <- Members, A =/= B].

%% @private Make database URL from node URL and database name
make_db_url(NodeUrl, Database) when is_binary(NodeUrl), is_binary(Database) ->
    case binary:match(NodeUrl, <<"://">>) of
        nomatch ->
            %% Local database name
            Database;
        _ ->
            %% Remote URL - append database path
            case binary:last(NodeUrl) of
                $/ -> <<NodeUrl/binary, Database/binary>>;
                _ -> <<NodeUrl/binary, "/db/", Database/binary>>
            end
    end.

%% @private Start a replication task
start_replication_task(Config) ->
    #{source := Source, target := Target} = Config,
    Mode = maps:get(mode, Config, one_shot),
    Filter = maps:get(filter, Config, #{}),
    Auth = maps:get(auth, Config, #{}),

    %% Apply auth to remote URLs
    SourceEndpoint = apply_auth_to_url(Source, Auth),
    TargetEndpoint = apply_auth_to_url(Target, Auth),

    %% Determine transports based on URL format
    SourceTransport = url_to_transport(Source),
    TargetTransport = url_to_transport(Target),

    TaskConfig = #{
        source => SourceEndpoint,
        target => TargetEndpoint,
        mode => Mode,
        source_transport => SourceTransport,
        target_transport => TargetTransport,
        filter => Filter
    },

    case Mode of
        one_shot ->
            %% Run once and return
            Opts = #{
                source_transport => SourceTransport,
                target_transport => TargetTransport,
                filter => Filter
            },
            case barrel_rep:replicate(SourceEndpoint, TargetEndpoint, Opts) of
                {ok, _Result} -> {ok, undefined};  % No long-running task
                Error -> Error
            end;
        continuous ->
            %% Start a continuous replication task
            case barrel_rep_tasks:start_task(TaskConfig) of
                {ok, TaskId} ->
                    %% Get the task to find the pid for monitoring
                    %% Note: TaskId is a binary, not a pid
                    {ok, TaskId};
                Error ->
                    Error
            end
    end.

%% @private Apply auth config to a URL if it's remote
apply_auth_to_url(Url, Auth) when is_binary(Url), map_size(Auth) > 0 ->
    case is_remote_url(Url) of
        true ->
            %% Build endpoint map with auth
            Endpoint = #{url => Url},
            apply_auth_config(Endpoint, Auth);
        false ->
            Url
    end;
apply_auth_to_url(Url, _Auth) ->
    Url.

%% @private Check if URL is remote (has http(s):// scheme)
is_remote_url(<<"http://", _/binary>>) -> true;
is_remote_url(<<"https://", _/binary>>) -> true;
is_remote_url(_) -> false.

%% @private Apply auth config to endpoint map
apply_auth_config(Endpoint, #{bearer_token := Token}) ->
    Endpoint#{bearer_token => Token};
apply_auth_config(Endpoint, #{<<"bearer_token">> := Token}) ->
    Endpoint#{bearer_token => Token};
apply_auth_config(Endpoint, #{basic_auth := {User, Pass}}) ->
    Endpoint#{basic_auth => {User, Pass}};
apply_auth_config(Endpoint, #{<<"basic_auth">> := #{<<"user">> := User, <<"pass">> := Pass}}) ->
    Endpoint#{basic_auth => {User, Pass}};
apply_auth_config(Endpoint, _) ->
    Endpoint.

%% @private Determine transport module from URL
url_to_transport(Url) when is_binary(Url) ->
    case binary:match(Url, <<"://">>) of
        nomatch -> barrel_rep_transport_local;
        _ -> barrel_rep_transport_http
    end.

%% @private Collect results from task start attempts
collect_task_results(Results) ->
    {Oks, Errors} = lists:partition(
        fun({ok, _}) -> true; (_) -> false end,
        Results
    ),
    case Errors of
        [] ->
            Pids = [Pid || {ok, Pid} <- Oks, Pid =/= undefined],
            {ok, Pids};
        [FirstError | _] ->
            FirstError
    end.

%% @private Stop all replication tasks for a policy
stop_policy_tasks(Name, State) ->
    case maps:find(Name, State#state.tasks) of
        {ok, TaskIds} ->
            lists:foreach(
                fun(TaskId) when is_binary(TaskId) ->
                    barrel_rep_tasks:stop_task(TaskId);
                   (_) -> ok
                end,
                TaskIds
            ),
            State#state{tasks = maps:remove(Name, State#state.tasks)};
        error ->
            State
    end.

%% @private Get status of a task
task_status(TaskId) when is_binary(TaskId) ->
    case barrel_rep_tasks:get_task(TaskId) of
        {ok, Info} -> Info;
        _ -> #{task_id => TaskId, status => unknown}
    end;
task_status(_) ->
    #{status => invalid}.

%%====================================================================
%% Persistence
%%====================================================================

%% @private Load policies from persistent storage
load_policies() ->
    case barrel_docdb:get_system_doc(?POLICY_PREFIX) of
        {ok, Doc} ->
            PolicyList = maps:get(<<"policies">>, Doc, []),
            lists:foldl(
                fun(PolicyMap, Acc) ->
                    Name = maps:get(<<"name">>, PolicyMap),
                    Policy = decode_policy(PolicyMap),
                    maps:put(Name, Policy, Acc)
                end,
                #{},
                PolicyList
            );
        {error, not_found} ->
            #{};
        {error, _Reason} ->
            #{}
    end.

%% @private Save a policy to persistent storage
save_policy(Policy) ->
    %% Load existing, update, save
    Policies = load_policies(),
    Name = maps:get(name, Policy),
    NewPolicies = maps:put(Name, Policy, Policies),
    Doc = #{
        <<"policies">> => [encode_policy(P) || P <- maps:values(NewPolicies)]
    },
    barrel_docdb:put_system_doc(?POLICY_PREFIX, Doc).

%% @private Delete a policy from storage
delete_policy_storage(Name) ->
    Policies = load_policies(),
    NewPolicies = maps:remove(Name, Policies),
    Doc = #{
        <<"policies">> => [encode_policy(P) || P <- maps:values(NewPolicies)]
    },
    barrel_docdb:put_system_doc(?POLICY_PREFIX, Doc).

%% @private Encode policy for storage
encode_policy(Policy) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
            Acc#{atom_to_binary(K) => encode_value(V)};
           (K, V, Acc) ->
            Acc#{K => encode_value(V)}
        end,
        #{},
        Policy
    ).

encode_value(V) when is_atom(V) -> atom_to_binary(V);
encode_value(V) when is_list(V) -> [encode_value(E) || E <- V];
encode_value(V) when is_map(V) -> encode_policy(V);
encode_value(V) -> V.

%% @private Decode policy from storage
decode_policy(Doc) ->
    maps:fold(
        fun(<<"pattern">>, V, Acc) ->
            Acc#{pattern => binary_to_atom(V)};
           (<<"enabled">>, V, Acc) ->
            Acc#{enabled => to_boolean(V)};
           (<<"mode">>, V, Acc) when is_binary(V) ->
            Acc#{mode => binary_to_atom(V)};
           (<<"name">>, V, Acc) ->
            Acc#{name => V};
           (<<"nodes">>, V, Acc) ->
            Acc#{nodes => V};
           (<<"database">>, V, Acc) ->
            Acc#{database => V};
           (<<"members">>, V, Acc) ->
            Acc#{members => V};
           (<<"source">>, V, Acc) ->
            Acc#{source => V};
           (<<"targets">>, V, Acc) ->
            Acc#{targets => V};
           (<<"hot_db">>, V, Acc) ->
            Acc#{hot_db => V};
           (<<"warm_db">>, V, Acc) ->
            Acc#{warm_db => V};
           (<<"cold_db">>, V, Acc) ->
            Acc#{cold_db => V};
           (<<"filter">>, V, Acc) ->
            Acc#{filter => V};
           (<<"auth">>, V, Acc) when is_map(V) ->
            Acc#{auth => V};
           (_, _, Acc) ->
            Acc
        end,
        #{},
        Doc
    ).

%% @private Convert various representations to boolean
to_boolean(true) -> true;
to_boolean(false) -> false;
to_boolean(<<"true">>) -> true;
to_boolean(<<"false">>) -> false;
to_boolean("true") -> true;
to_boolean("false") -> false;
to_boolean(_) -> false.
