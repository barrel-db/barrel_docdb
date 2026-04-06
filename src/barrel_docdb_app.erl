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
    %% Install instrument logger filter for trace context enrichment
    ok = instrument_logger:install(),
    %% Configure tracing
    ok = configure_tracing(),
    logger:info("Starting barrel_docdb application"),
    %% Configure jose JSON module (use OTP json)
    jose:json_module(json),
    %% Initialize JWT validation (loads console public key if configured)
    _ = barrel_docdb_jwt:init(),
    barrel_docdb_sup:start_link().

%% @doc Stop the barrel_docdb application
-spec stop(term()) -> ok.
stop(_State) ->
    logger:info("Stopping barrel_docdb application"),
    %% Shutdown span exporter
    ok = instrument_exporter:shutdown(),
    %% Uninstall instrument logger filter
    instrument_logger:uninstall(),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Configure tracing based on application environment.
%%
%% Configuration options:
%% - {tracing, enabled} - Enable/disable tracing (default: true)
%% - {tracing, exporter} - console | otlp | {module, Config} (default: none)
%% - {tracing, otlp_endpoint} - OTLP endpoint URL (for otlp exporter)
%%
%% Example sys.config:
%% ```
%% {barrel_docdb, [
%%     {tracing, [
%%         {enabled, true},
%%         {exporter, console}  %% or otlp or none
%%     ]}
%% ]}
%% '''
-spec configure_tracing() -> ok.
configure_tracing() ->
    TracingConfig = application:get_env(barrel_docdb, tracing, []),
    Enabled = proplists:get_value(enabled, TracingConfig, true),
    case Enabled of
        true ->
            configure_exporter(TracingConfig);
        false ->
            ok
    end.

%% @doc Configure the span exporter based on configuration.
configure_exporter(TracingConfig) ->
    Exporter = proplists:get_value(exporter, TracingConfig, none),
    case Exporter of
        none ->
            ok;
        console ->
            Format = proplists:get_value(console_format, TracingConfig, text),
            _ = instrument_exporter:register(instrument_exporter_console:new(#{
                format => Format
            })),
            logger:info("Tracing enabled with console exporter"),
            ok;
        otlp ->
            Endpoint = proplists:get_value(otlp_endpoint, TracingConfig, "http://localhost:4318"),
            _ = instrument_exporter:register(instrument_exporter_otlp:new(#{
                endpoint => Endpoint
            })),
            logger:info("Tracing enabled with OTLP exporter, endpoint: ~s", [Endpoint]),
            ok;
        {Module, Config} when is_atom(Module), is_map(Config) ->
            _ = instrument_exporter:register(#{module => Module, config => Config}),
            logger:info("Tracing enabled with custom exporter: ~p", [Module]),
            ok;
        _ ->
            logger:warning("Unknown tracing exporter configuration: ~p", [Exporter]),
            ok
    end.
