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
        {group, peer_management},
        {group, tagging},
        {group, member_resolution}
    ].

groups() ->
    [
        {basic, [], [
            node_id_persistent,
            node_info_contains_required_fields
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
%% Test Cases - Peer Management
%%====================================================================

add_peer_valid_url(_Config) ->
    Url = <<"http://example.com:8080">>,
    ok = barrel_discovery:add_peer(Url),
    %% Verify peer was added (status may be unreachable since we can't connect)
    {ok, PeerInfo} = barrel_discovery:get_peer(Url),
    ?assertEqual(Url, maps:get(url, PeerInfo)),
    ?assert(lists:member(maps:get(status, PeerInfo), [active, unreachable])),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

add_peer_invalid_url(_Config) ->
    %% Missing host
    ?assertMatch({error, {invalid_remote_url, _}},
                 barrel_discovery:add_peer(<<"not-a-url">>)),
    %% Missing protocol
    ?assertMatch({error, {invalid_remote_url, _}},
                 barrel_discovery:add_peer(<<"example.com">>)).

remove_peer(_Config) ->
    Url = <<"http://test-peer.example.com:9000">>,
    ok = barrel_discovery:add_peer(Url),
    %% Verify it exists
    {ok, _} = barrel_discovery:get_peer(Url),
    %% Remove it
    ok = barrel_discovery:remove_peer(Url),
    %% Verify it's gone
    ?assertEqual({error, not_found}, barrel_discovery:get_peer(Url)).

list_peers(_Config) ->
    %% Add two peers
    Url1 = <<"http://peer1.example.com:8080">>,
    Url2 = <<"http://peer2.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url1),
    ok = barrel_discovery:add_peer(Url2),
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
    Url = <<"http://tagged-peer.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url, #{tags => [<<"region-us">>, <<"production">>]}),
    {ok, PeerInfo} = barrel_discovery:get_peer(Url),
    Tags = maps:get(tags, PeerInfo, []),
    ?assert(lists:member(<<"region-us">>, Tags)),
    ?assert(lists:member(<<"production">>, Tags)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

list_peers_filtered(_Config) ->
    Url1 = <<"http://filter-test1.example.com:8080">>,
    Url2 = <<"http://filter-test2.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url1, #{tags => [<<"test-tag">>]}),
    ok = barrel_discovery:add_peer(Url2),  % No tags
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
    Url = <<"http://tag-test.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url),
    %% Add tag
    ok = barrel_discovery:tag_peer(Url, <<"new-tag">>),
    {ok, PeerInfo} = barrel_discovery:get_peer(Url),
    ?assert(lists:member(<<"new-tag">>, maps:get(tags, PeerInfo, []))),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url).

untag_peer(_Config) ->
    Url = <<"http://untag-test.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url, #{tags => [<<"tag-to-remove">>]}),
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
    Url1 = <<"http://list-tags1.example.com:8080">>,
    Url2 = <<"http://list-tags2.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url1, #{tags => [<<"alpha">>, <<"beta">>]}),
    ok = barrel_discovery:add_peer(Url2, #{tags => [<<"beta">>, <<"gamma">>]}),
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
    %% Direct URL should resolve to itself
    Url = <<"http://direct.example.com:8080">>,
    {ok, [ResolvedUrl]} = barrel_discovery:resolve_member(Url),
    ?assertEqual(Url, ResolvedUrl).

resolve_tag_reference(_Config) ->
    %% Add peers with a tag
    Url1 = <<"http://resolve-tag1.example.com:8080">>,
    Url2 = <<"http://resolve-tag2.example.com:8080">>,
    ok = barrel_discovery:add_peer(Url1, #{tags => [<<"resolve-test">>]}),
    ok = barrel_discovery:add_peer(Url2, #{tags => [<<"resolve-test">>]}),
    %% Resolve tag reference
    {ok, ResolvedUrls} = barrel_discovery:resolve_member({tag, <<"resolve-test">>}),
    ?assert(lists:member(Url1, ResolvedUrls)),
    ?assert(lists:member(Url2, ResolvedUrls)),
    %% Cleanup
    ok = barrel_discovery:remove_peer(Url1),
    ok = barrel_discovery:remove_peer(Url2).
