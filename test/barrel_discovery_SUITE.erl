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
        {group, peer_management}
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
            remove_peer,
            list_peers
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
