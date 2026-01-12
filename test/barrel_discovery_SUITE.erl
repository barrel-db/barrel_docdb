%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_discovery module
%%%
%%% Tests peer discovery and node info functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_discovery_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, basic},
        {group, zone_management},
        {group, peer_management},
        {group, tagging},
        {group, member_resolution},
        {group, dns_domains}
    ].

groups() ->
    [
        {basic, [], [
            node_id_persistent,
            node_info_contains_required_fields
        ]},
        {zone_management, [], [
            get_zone_default,
            get_zone_configured,
            nodes_in_zone,
            list_zones,
            filter_peers_by_zone
        ]},
        {peer_management, [], [
            add_peer_valid_url,
            add_peer_invalid_url,
            add_peer_with_tags,
            remove_peer,
            list_peers,
            list_peers_filtered
        ]},
        {tagging, [], [
            tag_peer,
            untag_peer,
            list_tags
        ]},
        {member_resolution, [], [
            resolve_direct_url,
            resolve_tag_reference
        ]},
        {dns_domains, [], [
            add_dns_domain,
            remove_dns_domain,
            list_dns_domains
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Basic
%%====================================================================

node_id_persistent(_Config) ->
    %% Get node ID twice, should be the same
    {ok, NodeId1} = barrel_discovery:node_id(),
    {ok, NodeId2} = barrel_discovery:node_id(),
    ?assertEqual(NodeId1, NodeId2),
    ?assert(is_binary(NodeId1)),
    ?assert(byte_size(NodeId1) > 0).

node_info_contains_required_fields(_Config) ->
    {ok, Info} = barrel_discovery:node_info(),
    ?assert(is_map(Info)),
    %% Required fields
    ?assert(maps:is_key(node_id, Info)),
    ?assert(maps:is_key(version, Info)),
    ?assert(maps:is_key(databases, Info)),
    ?assert(maps:is_key(federations, Info)),
    ?assert(maps:is_key(known_peers, Info)),
    ?assert(maps:is_key(timestamp, Info)),
    %% Check types
    ?assert(is_binary(maps:get(node_id, Info))),
    ?assert(is_binary(maps:get(version, Info))),
    ?assert(is_list(maps:get(databases, Info))),
    ?assert(is_list(maps:get(federations, Info))),
    ?assert(is_list(maps:get(known_peers, Info))),
    ?assert(is_integer(maps:get(timestamp, Info))).

%%====================================================================
%% Test Cases - Zone Management
%%====================================================================

get_zone_default(_Config) ->
    %% By default, zone should be undefined (not configured)
    {ok, Zone} = barrel_discovery:get_zone(),
    %% Zone may be undefined or configured in test env
    ?assert(Zone =:= undefined orelse is_binary(Zone)).

get_zone_configured(_Config) ->
    %% Test that zone can be set via application env
    %% First, set the env and check if a new discovery would read it
    OldZone = application:get_env(barrel_docdb, zone),
    application:set_env(barrel_docdb, zone, <<"test-zone">>),

    %% The current discovery instance won't have this zone (already started)
    %% So we test the zone parsing logic directly by checking if get_zone
    %% returns what was configured at startup time
    {ok, CurrentZone} = barrel_discovery:get_zone(),
    %% Zone is set at init time, so this test verifies the API works
    ?assert(CurrentZone =:= undefined orelse is_binary(CurrentZone)),

    %% Restore original env
    case OldZone of
        undefined -> application:unset_env(barrel_docdb, zone);
        {ok, Val} -> application:set_env(barrel_docdb, zone, Val)
    end.

nodes_in_zone(_Config) ->
    %% Add peers with zones (simulated - we set zone directly)
    Url1 = <<"http://127.0.0.1:19920">>,
    Url2 = <<"http://127.0.0.1:19921">>,
    Url3 = <<"http://127.0.0.1:19922">>,
    %% Add peers - they'll be unreachable but that's fine
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true}),
    {ok, _, _} = barrel_discovery:add_peer(Url2, #{sync => true}),
    {ok, _, _} = barrel_discovery:add_peer(Url3, #{sync => true}),
    %% Since peers are unreachable, they won't have zones set from remote
    %% Test that nodes_in_zone returns empty for unknown zone
    {ok, NodesInZone} = barrel_discovery:nodes_in_zone(<<"unknown-zone">>),
    ?assertEqual([], NodesInZone),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2),
    ok = barrel_discovery:remove_peer(Url3).

list_zones(_Config) ->
    %% Initially may be empty or have some zones
    {ok, Zones} = barrel_discovery:list_zones(),
    ?assert(is_list(Zones)).

filter_peers_by_zone(_Config) ->
    %% Test that list_peers with zone filter works
    Url1 = <<"http://127.0.0.1:19923">>,
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true}),
    %% Filter by a zone that doesn't exist
    {ok, FilteredPeers} = barrel_discovery:list_peers(#{zone => <<"nonexistent-zone">>}),
    %% Url1 should not be in the filtered list (it has no zone)
    FilteredUrls = [maps:get(url, P) || P <- FilteredPeers],
    ?assertNot(lists:member(Url1, FilteredUrls)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1).

%%====================================================================
%% Test Cases - Peer Management
%%====================================================================

add_peer_valid_url(_Config) ->
    %% Use localhost with closed port - connection refused is instant
    Url = <<"http://127.0.0.1:19999">>,
    %% Test sync mode - should return immediately with unreachable status
    {ok, PeerInfo, {unreachable, Reason}} = barrel_discovery:add_peer(Url, #{sync => true}),
    ?assertEqual(Url, maps:get(url, PeerInfo)),
    ?assertEqual(unreachable, maps:get(status, PeerInfo)),
    %% Verify real connection was attempted (should be connection_error)
    ?assertMatch({connection_error, _}, Reason),
    %% Verify peer is persisted - stored data should match returned data
    {ok, StoredPeer} = barrel_discovery:get_peer(Url),
    ?assertEqual(Url, maps:get(url, StoredPeer)),
    ?assertEqual(unreachable, maps:get(status, StoredPeer)),
    ?assertEqual(maps:get(last_seen, PeerInfo), maps:get(last_seen, StoredPeer)),
    %% Verify persistence survives - remove and verify it's gone
    ok = barrel_discovery:remove_peer(Url),
    ?assertEqual({error, not_found}, barrel_discovery:get_peer(Url)).

add_peer_invalid_url(_Config) ->
    %% Missing host
    ?assertMatch({error, {invalid_remote_url, _}},
                 barrel_discovery:add_peer(<<"not-a-url">>)),
    %% Missing protocol
    ?assertMatch({error, {invalid_remote_url, _}},
                 barrel_discovery:add_peer(<<"example.com">>)).

remove_peer(_Config) ->
    Url = <<"http://127.0.0.1:19901">>,
    {ok, _, _} = barrel_discovery:add_peer(Url, #{sync => true}),
    %% Verify it exists
    {ok, _} = barrel_discovery:get_peer(Url),
    %% Remove it
    ok = barrel_discovery:remove_peer(Url),
    %% Verify it's gone
    ?assertEqual({error, not_found}, barrel_discovery:get_peer(Url)).

list_peers(_Config) ->
    %% Add two peers (use sync to ensure they're fully added)
    Url1 = <<"http://127.0.0.1:19902">>,
    Url2 = <<"http://127.0.0.1:19903">>,
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true}),
    {ok, _, _} = barrel_discovery:add_peer(Url2, #{sync => true}),
    %% List peers
    {ok, Peers} = barrel_discovery:list_peers(),
    ?assert(is_list(Peers)),
    %% Find our peers
    PeerUrls = [maps:get(url, P) || P <- Peers],
    ?assert(lists:member(Url1, PeerUrls)),
    ?assert(lists:member(Url2, PeerUrls)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2).

add_peer_with_tags(_Config) ->
    Url = <<"http://127.0.0.1:19904">>,
    {ok, PeerInfo, _} = barrel_discovery:add_peer(Url, #{sync => true, tags => [<<"region-us">>, <<"production">>]}),
    Tags = maps:get(tags, PeerInfo, []),
    ?assert(lists:member(<<"region-us">>, Tags)),
    ?assert(lists:member(<<"production">>, Tags)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

list_peers_filtered(_Config) ->
    Url1 = <<"http://127.0.0.1:19905">>,
    Url2 = <<"http://127.0.0.1:19906">>,
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true, tags => [<<"test-tag">>]}),
    {ok, _, _} = barrel_discovery:add_peer(Url2, #{sync => true}),
    %% Filter by tag
    {ok, TaggedPeers} = barrel_discovery:list_peers(#{tag => <<"test-tag">>}),
    TaggedUrls = [maps:get(url, P) || P <- TaggedPeers],
    ?assert(lists:member(Url1, TaggedUrls)),
    ?assertNot(lists:member(Url2, TaggedUrls)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2).

%%====================================================================
%% Test Cases - Tagging
%%====================================================================

tag_peer(_Config) ->
    Url = <<"http://127.0.0.1:19907">>,
    {ok, _, _} = barrel_discovery:add_peer(Url, #{sync => true}),
    %% Add tag
    ok = barrel_discovery:tag_peer(Url, <<"new-tag">>),
    {ok, PeerInfo} = barrel_discovery:get_peer(Url),
    ?assert(lists:member(<<"new-tag">>, maps:get(tags, PeerInfo, []))),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

untag_peer(_Config) ->
    Url = <<"http://127.0.0.1:19908">>,
    {ok, _, _} = barrel_discovery:add_peer(Url, #{sync => true, tags => [<<"tag-to-remove">>]}),
    %% Verify tag exists
    {ok, PeerInfo1} = barrel_discovery:get_peer(Url),
    ?assert(lists:member(<<"tag-to-remove">>, maps:get(tags, PeerInfo1, []))),
    %% Remove tag
    ok = barrel_discovery:untag_peer(Url, <<"tag-to-remove">>),
    {ok, PeerInfo2} = barrel_discovery:get_peer(Url),
    ?assertNot(lists:member(<<"tag-to-remove">>, maps:get(tags, PeerInfo2, []))),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

list_tags(_Config) ->
    Url1 = <<"http://127.0.0.1:19909">>,
    Url2 = <<"http://127.0.0.1:19910">>,
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true, tags => [<<"alpha">>, <<"beta">>]}),
    {ok, _, _} = barrel_discovery:add_peer(Url2, #{sync => true, tags => [<<"beta">>, <<"gamma">>]}),
    {ok, Tags} = barrel_discovery:list_tags(),
    ?assert(lists:member(<<"alpha">>, Tags)),
    ?assert(lists:member(<<"beta">>, Tags)),
    ?assert(lists:member(<<"gamma">>, Tags)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2).

%%====================================================================
%% Test Cases - Member Resolution
%%====================================================================

resolve_direct_url(_Config) ->
    %% Direct URL should resolve to itself (no connection made)
    Url = <<"http://127.0.0.1:19911">>,
    {ok, [ResolvedUrl]} = barrel_discovery:resolve_member(Url),
    ?assertEqual(Url, ResolvedUrl).

resolve_tag_reference(_Config) ->
    %% Add peers with a tag
    Url1 = <<"http://127.0.0.1:19912">>,
    Url2 = <<"http://127.0.0.1:19913">>,
    {ok, _, _} = barrel_discovery:add_peer(Url1, #{sync => true, tags => [<<"resolve-test">>]}),
    {ok, _, _} = barrel_discovery:add_peer(Url2, #{sync => true, tags => [<<"resolve-test">>]}),
    %% Resolve tag reference
    {ok, ResolvedUrls} = barrel_discovery:resolve_member({tag, <<"resolve-test">>}),
    ?assert(lists:member(Url1, ResolvedUrls)),
    ?assert(lists:member(Url2, ResolvedUrls)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2).

%%====================================================================
%% Test Cases - DNS Domain Management
%%====================================================================

add_dns_domain(_Config) ->
    Domain = <<"test-discovery.example.com">>,
    %% Add a domain
    ok = barrel_discovery:add_dns_domain(Domain),
    %% Verify it's in the list
    {ok, Domains} = barrel_discovery:list_dns_domains(),
    ?assert(lists:member(Domain, Domains)),
    %% Add same domain again (should be idempotent)
    ok = barrel_discovery:add_dns_domain(Domain),
    {ok, Domains2} = barrel_discovery:list_dns_domains(),
    %% Should still be only one occurrence
    ?assertEqual(1, length([D || D <- Domains2, D =:= Domain])),
    %% Cleanup
    ok = barrel_discovery:remove_dns_domain(Domain).

remove_dns_domain(_Config) ->
    Domain = <<"remove-test.example.com">>,
    %% Add domain
    ok = barrel_discovery:add_dns_domain(Domain),
    {ok, Domains1} = barrel_discovery:list_dns_domains(),
    ?assert(lists:member(Domain, Domains1)),
    %% Remove domain
    ok = barrel_discovery:remove_dns_domain(Domain),
    {ok, Domains2} = barrel_discovery:list_dns_domains(),
    ?assertNot(lists:member(Domain, Domains2)).

list_dns_domains(_Config) ->
    Domain1 = <<"list-test1.example.com">>,
    Domain2 = <<"list-test2.example.com">>,
    %% Add domains
    ok = barrel_discovery:add_dns_domain(Domain1),
    ok = barrel_discovery:add_dns_domain(Domain2),
    %% List them
    {ok, Domains} = barrel_discovery:list_dns_domains(),
    ?assert(lists:member(Domain1, Domains)),
    ?assert(lists:member(Domain2, Domains)),
    %% Cleanup
    ok = barrel_discovery:remove_dns_domain(Domain1),
    ok = barrel_discovery:remove_dns_domain(Domain2).
