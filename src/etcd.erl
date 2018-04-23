-module(etcd).

-export([start/0, stop/0]).
-export([set/4, set/5, refresh_ttl/4]).
-export([test_and_set/5, test_and_set/6]).
-export([get/3, ls/3]).
-export([delete/3]).
-export([watch/3, watch/4]).
-export([sadd/4, sadd/5, sdel/4, sismember/4, smembers/3]).
-compile(export_all).
-include("include/etcd_types.hrl").

%% @doc Start application with all depencies
-spec start() -> ok | {error, term()}.
start() ->
    case application:ensure_all_started(etcd) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop application
-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(etcd).

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec set(url(), key(), value(), pos_timeout()) -> result().
set(Url, Key, Value, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = put_request(FullUrl, [{"value", Value}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec set(url(), key(), value(), pos_integer(), pos_timeout()) -> result().
set(Url, Key, Value, TTL, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = put_request(FullUrl, [{"value", Value}, {"ttl", TTL}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
refresh_ttl(Url, Key, TTL, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = put_request(FullUrl, [{"ttl", TTL}, {"refresh", "true"}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec test_and_set(url(), key(), value(), value(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = put_request(FullUrl, [{"value", Value}, {"prevValue", PrevValue}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec test_and_set(url(), key(), value(), value(), pos_integer(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, TTL, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = put_request(FullUrl, [{"value", Value}, {"prevValue", PrevValue}, {"ttl", TTL}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec get(url(), key(), pos_timeout()) -> result().
get(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec delete(url(), key(), pos_timeout()) -> result().
delete(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = lhttpc:request(FullUrl, delete, [], Timeout),
    handle_request_result(Result).

ls(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key) ++ "?quorum=false&recursive=true&sorted=false",
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec watch(url(), key(), pos_timeout()) -> result().
watch(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key) ++
                "?wait=true&recursive=true",
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Index, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Index = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec watch(url(), key(), pos_integer(), pos_timeout()) -> result().
watch(Url, Key, Index, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key) ++
                "?wait=true&recursive=true&waitIndex=" ++ convert_to_string(Index),
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

-spec sadd(url(), key(), value(), pos_timeout()) -> ok | {error, any()}.
sadd(Url, Key, Value, Timeout) -> etcd_sets:add(Url, Key, Value, Timeout).

-spec sadd(url(), key(), value(), pos_integer(), pos_timeout()) -> ok | {error, any()}.
sadd(Url, Key, Value, TTL, Timeout) -> etcd_sets:add(Url, Key, Value, TTL, Timeout).

-spec sdel(url(), key(), value(), pos_timeout()) -> ok | {error, any()}.
sdel(Url, Key, Value, Timeout) -> etcd_sets:del(Url, Key, Value, Timeout).

-spec sismember(url(), key(), value(), pos_timeout()) -> {ok, boolean()} | {error, any()}.
sismember(Url, Key, Value, Timeout) -> etcd_sets:ismember(Url, Key, Value, Timeout).

-spec smembers(url(), key(), pos_timeout()) -> {ok, [binary()]} | {error, any()}.
smembers(Url, Key, Timeout) -> etcd_sets:members(Url, Key, Timeout).

%% @private
convert_to_string(Value) when is_integer(Value) ->
    integer_to_list(Value);
convert_to_string(Value) when is_binary(Value) ->
    binary_to_list(Value);
convert_to_string(Value) when is_list(Value) ->
    Value.

%% @private
encode_params(Pairs) ->
    List = [ http_uri:encode(convert_to_string(Key)) ++ "=" ++ http_uri:encode(convert_to_string(Value)) || {Key, Value} <- Pairs ],
    binary:list_to_bin(string:join(List, "&")).

%% @private
url_prefix(Url) ->
    Url ++ "/v2".

%% @private
put_request(Url, Pairs, Timeout) ->
    do_request(put, Url, Pairs, Timeout).

do_request(Method, Url, Pairs, Timeout) ->
    Body = encode_params(Pairs),
    Headers = [{"Content-Type", "application/x-www-form-urlencoded"}],
    lhttpc:request(Url, Method, Headers, Body, Timeout).

%% @private
parse_response(Pairs) when is_list(Pairs) ->
    IsError = lists:keyfind(<<"errorCode">>, 1, Pairs),
    case IsError of
        {_, ErrorCode} ->
            parse_error_response(Pairs, #error{ errorCode = ErrorCode });
        false ->
            parse_response_inner(Pairs)
    end.

%% @private
parse_response_inner(Pairs) ->
    {_, Action} = lists:keyfind(<<"action">>, 1, Pairs),
    case Action of
        Action when Action =:= <<"set">>;
                    Action =:= <<"compareAndSwap">> ->
            parse_set_response(Pairs, #set{});
        <<"get">> ->
            parse_get_response(Pairs, #get{});
        <<"expire">> ->
            parse_delete_response(Pairs, #delete{});
        <<"delete">> ->
            parse_delete_response(Pairs, #delete{})
    end.

%% @private
parse_error_response([], Acc) ->
    Acc;
parse_error_response([Pair | Tail], Acc) ->
    {_, ErrorCode, Message, Cause} = Acc,
    case Pair of
        {<<"message">>, Message1} ->
            parse_error_response(Tail, {error, ErrorCode, Message1, Cause});
        {<<"cause">>, Cause1} ->
            parse_error_response(Tail, {error, ErrorCode, Message, Cause1});
        _ ->
            parse_error_response(Tail, Acc)
    end.

%% @private
parse_node_response(Response) ->
    parse_node_response(Response, #node{}).
parse_node_response([], Acc) ->
    Acc;
parse_node_response([Pair | Tail], #node{} = Acc) ->
    case Pair of
        {<<"key">>, Key} ->
            parse_node_response(Tail, Acc#node{key = Key});
        {<<"dir">>, IsDir} when is_boolean(IsDir) ->
            parse_node_response(Tail, Acc#node{dir = IsDir});
        {<<"nodes">>, Nodes} ->
            parse_node_response(Tail,
                Acc#node{nodes = [ parse_node_response(N) || N <- Nodes ]});
        {<<"value">>, Value} ->
            parse_node_response(Tail, Acc#node{value = Value});
        {<<"modifiedIndex">>, Index} ->
            parse_node_response(Tail, Acc#node{modifiedIndex = Index});
        {<<"createdIndex">>, Index} ->
            parse_node_response(Tail, Acc#node{createdIndex = Index});
        _ ->
            parse_node_response(Tail, Acc)
    end.


parse_set_response([], Acc) ->
    Acc;
parse_set_response([Pair | Tail], Acc) ->
    case Pair of
        {<<"node">>, NodePairs} ->
            N = parse_node_response(NodePairs),
            parse_set_response(Tail, Acc#set{key = N#node.key,
                                             value = N#node.value,
                                             newKey = true,
                                             index = N#node.modifiedIndex});
        {<<"prevNode">>, NodePairs} ->
            N = parse_node_response(NodePairs),
            parse_set_response(Tail, Acc#set{key = N#node.key,
                                             prevValue = N#node.value,
                                             newKey = false});
        _ ->
            parse_set_response(Tail, Acc)
    end.

%% @private
parse_get_response([], Acc) ->
    Acc;
parse_get_response([Pair | Tail], Acc) ->
    case Pair of
        {<<"node">>, NodePairs} ->
            N = parse_node_response(NodePairs),
            parse_get_response(Tail, Acc#get{key = N#node.key,
                                             dir = N#node.dir,
                                             nodes = N#node.nodes,
                                             value = N#node.value,
                                             index = N#node.modifiedIndex});
        _ ->
            parse_get_response(Tail, Acc)
    end.

%% @private
parse_delete_response([], Acc) ->
    Acc;
parse_delete_response([Pair | Tail], Acc) ->
    case Pair of
        {<<"node">>, NodePairs} ->
            N = parse_node_response(NodePairs),
            parse_delete_response(Tail, Acc#delete{key = N#node.key,
                                                   index = N#node.modifiedIndex});
        {<<"prevNode">>, NodePairs} ->
            N = parse_node_response(NodePairs),
            parse_delete_response(Tail, Acc#delete{prevValue = N#node.value});
        _ ->
            parse_delete_response(Tail, Acc)
    end.

%% @private
handle_request_result(Result) ->
    case Result of
        {ok, {{Code, _ReasonPhrase}, _Hdrs, ResponseBody}} when Code < 300 ->
            Decoded = jsx:decode(ResponseBody),
            {ok, parse_response(Decoded)};
        {_, Reason} ->
            {http_error, Reason}
    end.

