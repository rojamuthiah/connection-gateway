-module(connection_gateway_app).

-behaviour(application).

-export([start/2, stop/1]).

-define(LOG_HANDLER, connection_gateway_file).
-define(DEFAULT_LOG_FILE, "logs/connection-gateway.log").

start(_StartType, _StartArgs) ->
    case setup_file_logging() of
        ok ->
            case enable_file_log_events() of
                {ok, PreviousLoggerConfig} ->
                    case connection_gateway_sup:start_link() of
                        {ok, Pid} ->
                            logger:info(
                                "Connection gateway started on node ~p; logs: ~ts",
                                [node(), log_file()]
                            ),
                            {ok, Pid, PreviousLoggerConfig};
                        Error ->
                            restore_logger_config(PreviousLoggerConfig),
                            _ = logger:remove_handler(?LOG_HANDLER),
                            Error
                    end;
                {error, Reason} ->
                    _ = logger:remove_handler(?LOG_HANDLER),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(PreviousLoggerConfig) ->
    logger:info("Connection gateway stopping on node ~p", [node()]),
    _ = logger:remove_handler(?LOG_HANDLER),
    restore_logger_config(PreviousLoggerConfig),
    ok.

enable_file_log_events() ->
    PreviousPrimaryConfig = logger:get_primary_config(),
    case logger:get_handler_config(default) of
        {ok, DefaultHandlerConfig} ->
            PreviousDefaultLevel = maps:get(level, DefaultHandlerConfig),
            case logger:set_primary_config(level, info) of
                ok ->
                    case logger:set_handler_config(default, level, warning) of
                        ok ->
                            {ok, {PreviousPrimaryConfig, PreviousDefaultLevel}};
                        {error, Reason} ->
                            _ = logger:set_primary_config(PreviousPrimaryConfig),
                            {error, {default_logger_configuration_failed, Reason}}
                    end;
                {error, Reason} ->
                    {error, {primary_logger_configuration_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {default_logger_unavailable, Reason}}
    end.

restore_logger_config({PreviousPrimaryConfig, PreviousDefaultLevel}) ->
    _ = logger:set_primary_config(PreviousPrimaryConfig),
    _ = logger:set_handler_config(default, level, PreviousDefaultLevel),
    ok.

setup_file_logging() ->
    LogFile = log_file(),
    case filelib:ensure_dir(LogFile) of
        ok ->
            case logger:add_handler(
                ?LOG_HANDLER,
                logger_std_h,
                #{
                    level => info,
                    config => #{file => LogFile},
                    formatter => {logger_formatter, #{single_line => true}}
                }
            ) of
                ok ->
                    ok;
                {error, {already_exist, ?LOG_HANDLER}} ->
                    ok;
                {error, Reason} ->
                    {error, {file_logger_start_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {log_directory_create_failed, LogFile, Reason}}
    end.

log_file() ->
    case os:getenv("CONNECTION_GATEWAY_LOG_FILE") of
        false ->
            ?DEFAULT_LOG_FILE;
        "" ->
            ?DEFAULT_LOG_FILE;
        LogFile ->
            LogFile
    end.
