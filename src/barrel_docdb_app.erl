%%%-------------------------------------------------------------------
%%% @doc barrel_docdb application module
%%%
%%% Starts and stops the barrel_docdb application.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

%% @doc Start the barrel_docdb application
-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    logger:info("Starting barrel_docdb application"),
    barrel_docdb_sup:start_link().

%% @doc Stop the barrel_docdb application
-spec stop(term()) -> ok.
stop(_State) ->
    logger:info("Stopping barrel_docdb application"),
    ok.
