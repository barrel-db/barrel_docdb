%%%-------------------------------------------------------------------
%%% @doc HTTP Server for barrel_docdb P2P replication
%%%
%%% Provides HTTP endpoints for:
%%% - Health checks
%%% - Document CRUD
%%% - Changes feed
%%% - Replication
%%%
%%% Supports both JSON and CBOR content types via Accept/Content-Type
%%% headers.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_server).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1]).
-export([stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 8080).
-define(DEFAULT_LISTENERS, 100).

-record(state, {
    listener_pid :: pid() | undefined,
    port :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the HTTP server with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the HTTP server with options.
%% Options:
%%   - port: Listen port (default: 8080)
%%   - num_acceptors: Number of acceptor processes (default: 100)
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Stop the HTTP server.
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    NumAcceptors = maps:get(num_acceptors, Opts, ?DEFAULT_LISTENERS),

    Dispatch = cowboy_router:compile([
        {'_', [
            %% Health endpoint
            {"/health", barrel_http_handler, #{action => health}},

            %% Database operations
            {"/db/:db", barrel_http_handler, #{action => db_info}},

            %% Static paths BEFORE variable paths
            %% Changes feed
            {"/db/:db/_changes", barrel_http_handler, #{action => changes}},

            %% Bulk operations
            {"/db/:db/_bulk_docs", barrel_http_handler, #{action => bulk_docs}},

            %% Replication
            {"/db/:db/_replicate", barrel_http_handler, #{action => replicate}},

            %% Document operations (variable path - must be last)
            {"/db/:db/:doc_id", barrel_http_handler, #{action => doc}}
        ]}
    ]),

    TransOpts = #{
        socket_opts => [{port, Port}],
        num_acceptors => NumAcceptors
    },

    ProtoOpts = #{
        env => #{dispatch => Dispatch},
        stream_handlers => [cowboy_stream_h]
    },

    case cowboy:start_clear(barrel_http_listener, TransOpts, ProtoOpts) of
        {ok, ListenerPid} ->
            logger:info("barrel_http_server started on port ~p", [Port]),
            {ok, #state{listener_pid = ListenerPid, port = Port}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    cowboy:stop_listener(barrel_http_listener),
    ok.
