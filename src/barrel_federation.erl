%%%-------------------------------------------------------------------
%%% @doc Cross-database query federation
%%%
%%% Enables querying across multiple databases with merged results.
%%% Federation configurations are stored as system documents and
%%% cached in persistent_term for fast access.
%%%
%%% Example usage:
%%% ```
%%% %% Create a federation spanning multiple databases
%%% ok = barrel_federation:create(<<"all_users">>, [
%%%     <<"users_db">>,
%%%     <<"archive_users_db">>
%%% ]).
%%%
%%% %% Create a federation using discovery references
%%% ok = barrel_federation:create(<<"distributed_users">>, [
%%%     <<"local_users">>,                          % Local database
%%%     <<"http://node2:8080">>,                    % Remote node
%%%     {tag, <<"region-us">>},                     % All peers tagged "region-us"
%%%     {all_peers, <<"users_db">>}                 % All peers with "users_db"
%%% ]).
%%%
%%% %% Query across all federation members
%%% {ok, Results, Meta} = barrel_federation:find(<<"all_users">>, #{
%%%     where => [{path, [<<"type">>], <<"user">>}]
%%% }).
%%%
%%% %% Delete federation
%%% ok = barrel_federation:delete(<<"all_users">>).
%%% '''
%%%
%%% Member types supported:
%%% - Binary: Local database name or remote URL
%%% - {peer, NodeId}: Peer identified by node ID from discovery
%%% - {peer, NodeId, DbName}: Specific database on a peer
%%% - {tag, TagName}: All discovered peers with this tag
%%% - {tag, TagName, DbName}: Peers with tag + specific database
%%% - {all_peers, DbName}: All discovered peers that have this database
%%%
%%% Results are merged by document ID. When the same document exists
%%% in multiple databases, the version with the highest revision wins.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_federation).

-compile({no_auto_import, [get/1]}).

%% API
-export([
    create/2,
    create/3,
    delete/1,
    get/1,
    get_safe/1,
    list/0,
    list_safe/0,
    find/1,
    find/2,
    find/3,
    add_member/2,
    remove_member/2,
    set_query/2
]).

%% Exported for testing
-export([validate_remote_url/1, add_auth_headers/2]).
-ignore_xref([validate_remote_url/1, add_auth_headers/2]).

%% Types
-export_type([federation/0, member/0]).

%%====================================================================
%% Types
%%====================================================================

-type federation_name() :: binary().
-type member() :: binary()                      % Local database name or remote URL
                | {peer, binary()}              % {peer, NodeId}
                | {peer, binary(), binary()}    % {peer, NodeId, DbName}
                | {tag, binary()}               % {tag, TagName}
                | {tag, binary(), binary()}     % {tag, TagName, DbName}
                | {all_peers, binary()}.        % {all_peers, DbName}

-type federation() :: #{
    name := federation_name(),
    members := [member()],
    created_at := integer(),
    options := map(),
    query => query_spec()  % Optional default query
}.

-type query_spec() :: map().

-type auth_config() :: #{
    bearer_token => binary(),
    basic_auth => {binary(), binary()}
}.

-type query_opts() :: #{
    timeout => pos_integer(),  % Query timeout per member (default: 30000ms)
    merge_strategy => newest | all,  % How to handle duplicates (default: newest)
    chunk_size => pos_integer(),
    auth => auth_config()  % Per-query auth override
}.

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new federation with default options
-spec create(federation_name(), [member()]) -> ok | {error, term()}.
create(Name, Members) ->
    create(Name, Members, #{}).

%% @doc Create a new federation with custom options
%% Options:
%%   - description: optional description
%%   - query: default query spec applied when find/2 is called
-spec create(federation_name(), [member()], map()) -> ok | {error, term()}.
create(Name, Members, Options) when is_binary(Name), is_list(Members) ->
    %% Validate members exist
    case validate_members(Members) of
        ok ->
            %% Extract query and auth from options if present
            Query = maps:get(query, Options, undefined),
            Auth = maps:get(auth, Options, undefined),
            CleanOptions = maps:without([query, auth], Options),

            Federation = #{
                name => Name,
                members => Members,
                created_at => erlang:system_time(millisecond),
                options => CleanOptions
            },

            %% Add query if specified
            FederationWithQuery = case Query of
                undefined -> Federation;
                _ -> Federation#{query => Query}
            end,

            %% Add auth if specified
            FederationWithAuth = case Auth of
                undefined -> FederationWithQuery;
                _ -> FederationWithQuery#{auth => Auth}
            end,

            %% Store as system document
            DocId = federation_doc_id(Name),
            case barrel_docdb:put_system_doc(DocId, FederationWithAuth) of
                ok ->
                    %% Cache in persistent_term
                    persistent_term:put({barrel_federation, Name}, FederationWithAuth),
                    %% Trigger discovery for remote members
                    trigger_discovery(Members),
                    ok;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Delete a federation
-spec delete(federation_name()) -> ok | {error, not_found}.
delete(Name) ->
    DocId = federation_doc_id(Name),
    %% Remove from cache
    persistent_term:erase({barrel_federation, Name}),
    %% Delete system doc
    case barrel_docdb:delete_system_doc(DocId) of
        ok -> ok;
        {error, not_found} -> {error, not_found}
    end.

%% @doc Get a federation by name
-spec get(federation_name()) -> {ok, federation()} | {error, not_found}.
get(Name) ->
    %% Try cache first
    case persistent_term:get({barrel_federation, Name}, undefined) of
        undefined ->
            %% Load from system doc
            DocId = federation_doc_id(Name),
            case barrel_docdb:get_system_doc(DocId) of
                {ok, Federation} ->
                    %% Cache it
                    persistent_term:put({barrel_federation, Name}, Federation),
                    {ok, Federation};
                {error, not_found} ->
                    {error, not_found}
            end;
        Federation ->
            {ok, Federation}
    end.

%% @doc List all federations
-spec list() -> {ok, [federation()]}.
list() ->
    %% Fold over system docs with federation prefix
    {ok, Federations}  = barrel_docdb:fold_system_docs(
        <<"federation:">>,
        fun(_DocId, Federation, Acc) -> [Federation | Acc] end,
        []
    ),
    {ok, lists:reverse(Federations)}.

%% @doc Get federation for API response (auth stripped)
-spec get_safe(federation_name()) -> {ok, federation()} | {error, not_found}.
get_safe(Name) ->
    case get(Name) of
        {ok, Federation} ->
            {ok, maps:remove(auth, Federation)};
        Error ->
            Error
    end.

%% @doc List federations for API response (auth stripped)
-spec list_safe() -> {ok, [federation()]}.
list_safe() ->
    {ok, Federations} = list(),
    {ok, [maps:remove(auth, F) || F <- Federations]}.

%% @doc Add a member to an existing federation
-spec add_member(federation_name(), member()) -> ok | {error, term()}.
add_member(Name, Member) ->
    case get(Name) of
        {ok, #{members := Members} = Federation} ->
            case lists:member(Member, Members) of
                true ->
                    ok;  % Already a member
                false ->
                    case validate_members([Member]) of
                        ok ->
                            NewMembers = Members ++ [Member],
                            update_federation(Name, Federation#{members => NewMembers});
                        {error, _} = Err ->
                            Err
                    end
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Remove a member from a federation
-spec remove_member(federation_name(), member()) -> ok | {error, term()}.
remove_member(Name, Member) ->
    case get(Name) of
        {ok, #{members := Members} = Federation} ->
            case lists:member(Member, Members) of
                false ->
                    ok;  % Not a member
                true ->
                    NewMembers = lists:delete(Member, Members),
                    update_federation(Name, Federation#{members => NewMembers})
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Execute the stored query across all federation members
%% Uses the default query stored in the federation config
-spec find(federation_name()) ->
    {ok, [map()], map()} | {error, term()}.
find(FederationName) ->
    case get(FederationName) of
        {ok, #{query := StoredQuery}} ->
            find(FederationName, StoredQuery, #{});
        {ok, _} ->
            %% No stored query - use empty query (all docs)
            find(FederationName, #{}, #{});
        {error, not_found} ->
            {error, {federation_not_found, FederationName}}
    end.

%% @doc Execute a query across all federation members
%% If federation has a stored query, merges with provided query
-spec find(federation_name(), query_spec()) ->
    {ok, [map()], map()} | {error, term()}.
find(FederationName, QuerySpec) ->
    find(FederationName, QuerySpec, #{}).

%% @doc Execute a query across all federation members with options
-spec find(federation_name(), query_spec(), query_opts()) ->
    {ok, [map()], map()} | {error, term()}.
find(FederationName, QuerySpec, Opts) ->
    Start = erlang:monotonic_time(millisecond),
    Result = case get(FederationName) of
        {ok, #{members := Members} = Federation} ->
            Timeout = maps:get(timeout, Opts, 30000),
            MergeStrategy = maps:get(merge_strategy, Opts, newest),

            %% Get auth from opts (per-query override) or from federation config
            Auth = case maps:get(auth, Opts, undefined) of
                undefined -> maps:get(auth, Federation, undefined);
                QueryAuth -> QueryAuth
            end,

            %% Merge stored query with provided query
            StoredQuery = maps:get(query, Federation, #{}),
            MergedQuery = merge_queries(StoredQuery, QuerySpec),

            %% Resolve member references (expand {peer, X}, {tag, Y}, etc.)
            ResolvedMembers = resolve_members(Members),

            %% Execute queries in parallel
            Results = parallel_query(ResolvedMembers, MergedQuery, Timeout, Auth),

            %% Merge results
            {MergedDocs, SourceCounts} = merge_results(Results, MergeStrategy),

            Meta = #{
                federation => FederationName,
                members_queried => length(ResolvedMembers),
                source_counts => SourceCounts,
                total_results => length(MergedDocs)
            },

            {ok, MergedDocs, Meta};
        {error, not_found} ->
            {error, {federation_not_found, FederationName}}
    end,
    %% Record federation metrics
    Duration = erlang:monotonic_time(millisecond) - Start,
    barrel_metrics:inc_federation_queries(FederationName),
    barrel_metrics:observe_federation_latency(FederationName, Duration),
    Result.

%% @doc Set or update the default query for a federation
-spec set_query(federation_name(), query_spec()) -> ok | {error, term()}.
set_query(Name, QuerySpec) ->
    case get(Name) of
        {ok, Federation} ->
            UpdatedFederation = Federation#{query => QuerySpec},
            update_federation(Name, UpdatedFederation);
        {error, not_found} ->
            {error, not_found}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Generate document ID for federation
federation_doc_id(Name) ->
    <<"federation:", Name/binary>>.

%% @private Validate that all members exist
%% Members can be:
%%   - Local database name: <<"mydb">>
%%   - Remote HTTP URL: <<"http://host:port/db/mydb">>
%%   - Domain for DNS discovery: <<"example.com">>
%%   - Discovery reference: {peer, NodeId}, {tag, Tag}, etc.
validate_members([]) ->
    ok;
validate_members([Member | Rest]) when is_binary(Member) ->
    case classify_member(Member) of
        {url, _} ->
            %% Remote URL - validate format only (can't check if it exists)
            case validate_remote_url(Member) of
                ok -> validate_members(Rest);
                {error, _} = Err -> Err
            end;
        {domain, _} ->
            %% Domain for DNS discovery - just validate format
            validate_members(Rest);
        local_db ->
            %% Local database name
            case barrel_docdb:db_pid(Member) of
                {ok, _Pid} ->
                    validate_members(Rest);
                {error, not_found} ->
                    {error, {member_not_found, Member}}
            end
    end;
%% Discovery references are validated at query time (dynamic resolution)
validate_members([{peer, NodeId} | Rest]) when is_binary(NodeId) ->
    validate_members(Rest);
validate_members([{peer, NodeId, DbName} | Rest])
  when is_binary(NodeId), is_binary(DbName) ->
    validate_members(Rest);
validate_members([{tag, Tag} | Rest]) when is_binary(Tag) ->
    validate_members(Rest);
validate_members([{tag, Tag, DbName} | Rest])
  when is_binary(Tag), is_binary(DbName) ->
    validate_members(Rest);
validate_members([{all_peers, DbName} | Rest]) when is_binary(DbName) ->
    validate_members(Rest);
validate_members([Invalid | _]) ->
    {error, {invalid_member, Invalid}}.

%% @private Resolve all member references to concrete URLs/db names
%% Called at query time to expand dynamic references
resolve_members(Members) ->
    lists:flatmap(fun resolve_member/1, Members).

resolve_member(Member) when is_binary(Member) ->
    case classify_member(Member) of
        {domain, Domain} ->
            %% Domain - resolve via DNS SRV, fall back to http://domain:8080
            case resolve_domain(Domain) of
                [] -> [<<"http://", Domain/binary, ":8080">>];  % Default fallback
                Urls -> Urls
            end;
        _ ->
            %% Direct URL or local db name - return as-is
            [Member]
    end;
resolve_member({peer, _} = Ref) ->
    resolve_via_discovery(Ref);
resolve_member({peer, _, _} = Ref) ->
    resolve_via_discovery(Ref);
resolve_member({tag, _} = Ref) ->
    resolve_via_discovery(Ref);
resolve_member({tag, _, _} = Ref) ->
    resolve_via_discovery(Ref);
resolve_member({all_peers, _} = Ref) ->
    resolve_via_discovery(Ref).

resolve_via_discovery(Ref) ->
    case barrel_discovery:resolve_member(Ref) of
        {ok, Urls} -> Urls;
        {error, _} -> []  % Skip failed resolutions
    end.

%% @private Check if member is a remote HTTP URL
is_remote_member(<<"http://", _/binary>>) -> true;
is_remote_member(<<"https://", _/binary>>) -> true;
is_remote_member(_) -> false.

%% @private Validate remote URL format
validate_remote_url(Url) ->
    case uri_string:parse(Url) of
        #{scheme := Scheme, host := Host}
            when (Scheme =:= <<"http">> orelse Scheme =:= <<"https">>),
                 Host =/= <<>> ->
            %% Path is optional - URLs like "http://host:8080" are valid
            ok;
        _ ->
            {error, {invalid_remote_url, Url}}
    end.

%% @private Update federation in storage and cache
update_federation(Name, Federation) ->
    DocId = federation_doc_id(Name),
    case barrel_docdb:put_system_doc(DocId, Federation) of
        ok ->
            persistent_term:put({barrel_federation, Name}, Federation),
            ok;
        {error, _} = Err ->
            Err
    end.

%% @private Execute query across members in parallel
parallel_query(Members, QuerySpec, Timeout, Auth) ->
    Parent = self(),
    Ref = make_ref(),

    %% Spawn a process for each member
    _Pids = lists:map(
        fun(Member) ->
            spawn_link(fun() ->
                Result = try
                    query_member(Member, QuerySpec, Timeout, Auth)
                catch
                    _:Reason ->
                        {error, Reason}
                end,
                Parent ! {Ref, Member, Result}
            end)
        end,
        Members
    ),

    %% Collect results with timeout
    collect_results(Ref, Members, Timeout, #{}).

%% @private Query a single member (local or remote)
query_member(Member, QuerySpec, Timeout, Auth) ->
    case is_remote_member(Member) of
        true ->
            query_remote_member(Member, QuerySpec, Timeout, Auth);
        false ->
            barrel_docdb:find(Member, QuerySpec)
    end.

%% @private Query a remote member via HTTP
query_remote_member(Url, QuerySpec, Timeout, Auth) ->
    %% Parse URL and add /_find endpoint
    FindUrl = <<Url/binary, "/_find">>,

    %% Encode query as JSON
    QueryJson = json:encode(query_spec_to_json(QuerySpec)),

    %% Make HTTP request using hackney
    BaseHeaders = [{<<"Content-Type">>, <<"application/json">>},
                   {<<"Accept">>, <<"application/json">>}],
    Headers = add_auth_headers(BaseHeaders, Auth),
    Options = [{recv_timeout, Timeout}, {connect_timeout, 5000}],

    case hackney:post(FindUrl, Headers, QueryJson, Options) of
        {ok, 200, RespHeaders, Body} ->
            _ = barrel_hlc:maybe_sync_from_header(
                proplists:get_value(<<"x-barrel-hlc">>, RespHeaders)),
            parse_remote_response(Body);
        {ok, Status, RespHeaders, Body} ->
            _ = barrel_hlc:maybe_sync_from_header(
                proplists:get_value(<<"x-barrel-hlc">>, RespHeaders)),
            {error, {http_error, Status, Body}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @private Add authentication headers based on auth config
add_auth_headers(Headers, #{bearer_token := Token}) ->
    [{<<"Authorization">>, <<"Bearer ", Token/binary>>} | Headers];
add_auth_headers(Headers, #{basic_auth := {User, Pass}}) ->
    Credentials = base64:encode(<<User/binary, ":", Pass/binary>>),
    [{<<"Authorization">>, <<"Basic ", Credentials/binary>>} | Headers];
add_auth_headers(Headers, _) ->
    Headers.

%% @private Convert query spec to JSON-encodable format
query_spec_to_json(QuerySpec) when is_map(QuerySpec) ->
    maps:fold(
        fun(where, Conditions, Acc) ->
            Acc#{<<"where">> => [condition_to_json(C) || C <- Conditions]};
           (limit, V, Acc) -> Acc#{<<"limit">> => V};
           (offset, V, Acc) -> Acc#{<<"offset">> => V};
           (order_by, V, Acc) -> Acc#{<<"order_by">> => V};
           (include_docs, V, Acc) -> Acc#{<<"include_docs">> => V};
           (K, V, Acc) -> Acc#{atom_to_binary(K, utf8) => V}
        end,
        #{},
        QuerySpec
    ).

%% @private Convert condition to JSON format
condition_to_json({path, Path, Value}) ->
    #{<<"path">> => Path, <<"value">> => Value};
condition_to_json({compare, Path, Op, Value}) ->
    #{<<"path">> => Path, <<"op">> => atom_to_binary(Op, utf8), <<"value">> => Value};
condition_to_json({'and', Conditions}) ->
    #{<<"and">> => [condition_to_json(C) || C <- Conditions]};
condition_to_json({'or', Conditions}) ->
    #{<<"or">> => [condition_to_json(C) || C <- Conditions]};
condition_to_json(Other) ->
    Other.

%% @private Parse response from remote member
parse_remote_response(Body) ->
    case json:decode(Body) of
        #{<<"results">> := Results, <<"meta">> := Meta} ->
            {ok, Results, Meta};
        #{<<"results">> := Results} ->
            {ok, Results, #{}};
        #{<<"error">> := Error} ->
            {error, Error};
        Other ->
            {error, {unexpected_response, Other}}
    end.

%% @private Collect results from parallel queries
collect_results(_Ref, [], _Timeout, Acc) ->
    Acc;
collect_results(Ref, Remaining, Timeout, Acc) ->
    receive
        {Ref, Member, Result} ->
            NewAcc = Acc#{Member => Result},
            NewRemaining = lists:delete(Member, Remaining),
            collect_results(Ref, NewRemaining, Timeout, NewAcc)
    after Timeout ->
        %% Mark remaining as timed out
        lists:foldl(
            fun(Member, A) ->
                A#{Member => {error, timeout}}
            end,
            Acc,
            Remaining
        )
    end.

%% @private Merge results from multiple sources
%% Returns {MergedDocs, SourceCounts}
merge_results(Results, MergeStrategy) ->
    %% Collect all docs with their source
    AllDocs = maps:fold(
        fun(Member, {ok, Docs, _Meta}, Acc) ->
            lists:foldl(
                fun(Doc, A) ->
                    DocId = maps:get(<<"id">>, Doc, maps:get(<<"_id">>, Doc, undefined)),
                    [{DocId, Member, Doc} | A]
                end,
                Acc,
                Docs
            );
           (_Member, {error, _}, Acc) ->
            Acc
        end,
        [],
        Results
    ),

    %% Group by document ID
    GroupedById = lists:foldl(
        fun({DocId, Member, Doc}, Acc) ->
            Existing = maps:get(DocId, Acc, []),
            Acc#{DocId => [{Member, Doc} | Existing]}
        end,
        #{},
        AllDocs
    ),

    %% Merge duplicates according to strategy
    {MergedDocs, SourceCounts} = maps:fold(
        fun(_DocId, Versions, {DocsAcc, CountsAcc}) ->
            {WinnerMember, WinnerDoc} = select_winner(Versions, MergeStrategy),
            NewCounts = maps:update_with(
                WinnerMember,
                fun(C) -> C + 1 end,
                1,
                CountsAcc
            ),
            {[WinnerDoc | DocsAcc], NewCounts}
        end,
        {[], #{}},
        GroupedById
    ),

    {MergedDocs, SourceCounts}.

%% @private Select winner from multiple versions of a document
select_winner([{Member, Doc}], _Strategy) ->
    {Member, Doc};
select_winner(Versions, newest) ->
    %% Select version with highest revision number
    lists:foldl(
        fun({Member, Doc}, {_AccMember, AccDoc} = Acc) ->
            DocRev = get_rev(Doc),
            AccRev = get_rev(AccDoc),
            case compare_revs(DocRev, AccRev) of
                greater -> {Member, Doc};
                _ -> Acc
            end
        end,
        hd(Versions),
        tl(Versions)
    );
select_winner(Versions, all) ->
    %% Just pick the first one (all strategy would need different return type)
    hd(Versions).

%% @private Extract revision from result (may be at top level or in <<"doc">>)
get_rev(Result) ->
    case maps:get(<<"_rev">>, Result, undefined) of
        undefined ->
            %% Try inside <<"doc">> if present
            case maps:get(<<"doc">>, Result, undefined) of
                undefined -> <<"0-">>;
                Doc -> maps:get(<<"_rev">>, Doc, <<"0-">>)
            end;
        Rev ->
            Rev
    end.

%% @private Compare two revision strings (e.g., "3-abc" vs "2-def")
compare_revs(Rev1, Rev2) ->
    Gen1 = rev_generation(Rev1),
    Gen2 = rev_generation(Rev2),
    if
        Gen1 > Gen2 -> greater;
        Gen1 < Gen2 -> less;
        Rev1 > Rev2 -> greater;  % Same gen, compare hash lexically
        Rev1 < Rev2 -> less;
        true -> equal
    end.

%% @private Extract generation number from revision string
rev_generation(Rev) when is_binary(Rev) ->
    case binary:split(Rev, <<"-">>) of
        [GenBin, _Hash] ->
            try binary_to_integer(GenBin)
            catch _:_ -> 0
            end;
        _ ->
            0
    end.

%% @private Merge two query specs
%% Provided query takes precedence, but 'where' conditions are combined
merge_queries(Stored, Provided) when map_size(Stored) =:= 0 ->
    Provided;
merge_queries(Stored, Provided) when map_size(Provided) =:= 0 ->
    Stored;
merge_queries(Stored, Provided) ->
    %% Start with provided query
    Merged = maps:merge(Stored, Provided),
    %% Combine 'where' conditions if both have them
    case {maps:get(where, Stored, undefined), maps:get(where, Provided, undefined)} of
        {undefined, _} -> Merged;
        {_, undefined} -> Merged;
        {StoredWhere, ProvidedWhere} ->
            %% Combine as AND - both conditions must match
            CombinedWhere = StoredWhere ++ ProvidedWhere,
            Merged#{where => CombinedWhere}
    end.

%% @private Trigger discovery for remote members
%% Called when federation is created to discover peers
trigger_discovery(Members) ->
    spawn(fun() ->
        lists:foreach(fun(Member) ->
            discover_member(Member)
        end, Members)
    end).

%% @private Discover a single member
discover_member(Member) when is_binary(Member) ->
    case classify_member(Member) of
        {url, Url} ->
            %% HTTP URL - discover via /.well-known/barrel
            catch barrel_discovery:discover_from(Url);
        {domain, Domain} ->
            %% Domain only - register for periodic DNS discovery and trigger initial lookup
            catch barrel_discovery:add_dns_domain(Domain);
        local_db ->
            %% Local database - no discovery needed
            ok
    end;
discover_member(_) ->
    %% Discovery references are resolved at query time
    ok.

%% @private Classify a member string
classify_member(<<"http://", _/binary>> = Url) -> {url, Url};
classify_member(<<"https://", _/binary>> = Url) -> {url, Url};
classify_member(Member) ->
    %% Check if it looks like a domain (has dots, no slashes)
    case {binary:match(Member, <<".">>), binary:match(Member, <<"/">>)} of
        {{_, _}, nomatch} ->
            %% Has dots but no slashes - treat as domain
            {domain, Member};
        _ ->
            %% Probably a local database name
            local_db
    end.

%% @private Resolve domain via DNS SRV lookup
%% Returns list of URLs from SRV records, or empty list if no records
resolve_domain(Domain) ->
    SrvName = "_barrel._tcp." ++ binary_to_list(Domain),
    case inet_res:lookup(SrvName, in, srv) of
        [] ->
            [];
        Records ->
            %% SRV record format: {Priority, Weight, Port, Host}
            %% Sort by priority (lower is better), then weight (higher is better)
            Sorted = lists:sort(
                fun({P1, W1, _, _}, {P2, W2, _, _}) ->
                    if P1 < P2 -> true;
                       P1 > P2 -> false;
                       W1 > W2 -> true;
                       true -> false
                    end
                end,
                Records
            ),
            [iolist_to_binary([
                <<"http://">>,
                list_to_binary(Host),
                <<":">>,
                integer_to_binary(Port)
            ]) || {_Priority, _Weight, Port, Host} <- Sorted]
    end.
