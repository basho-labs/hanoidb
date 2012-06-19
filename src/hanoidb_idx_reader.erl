%% ----------------------------------------------------------------------------
%%
%% hanoidb: LSM-trees (Log-Structured Merge Trees) Indexed Storage
%%
%% Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
%%
%% Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
%% http://basho.com/ info@basho.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(hanoidb_idx_reader).
-author('Kresten Krab Thorup <krab@trifork.com>').

%%
%% Streaming index file reader.
%%

-include_lib("kernel/include/file.hrl").
-include("include/hanoidb.hrl").
-include("hanoidb.hrl").
-include("include/plain_rpc.hrl").

-define(ASSERT_WHEN(X), when X).

-export([open/1, open/2,close/1,lookup/2,fold/3,range_fold/4, destroy/1]).
-export([first_node/1,next_node/1]).
-export([serialize/1, deserialize/1]).

-record(node, { level, members=[] }).
-record(index, {file, idxmgr, idxstate, name, config=[]}).

-type read_file() :: #index{}.

-spec open(Name::string()) -> read_file().
open(Name) ->
    open(Name, [random]).

-type config() :: [sequential | folding | random | {atom(), term()}].

-spec open(Name::string(), config()) -> read_file().
open(Name, Config) ->
    case proplists:get_bool(sequential, Config) of
        true ->
            ReadBufferSize = hanoidb:get_opt(read_buffer_size, Config, 512 * 1024),
            {ok, File} = file:open(Name, [raw,read,{read_ahead, ReadBufferSize},binary]),
            {ok, #index{file=File, name=Name, config=Config}};

        false ->
            case proplists:get_bool(folding, Config) of
                true ->
                    ReadBufferSize = hanoidb:get_opt(read_buffer_size, Config, 512 * 1024),
                    {ok, File} = file:open(Name, [read,{read_ahead, ReadBufferSize},binary]);
                false ->
                    {ok, File} = file:open(Name, [read,binary])
            end,

            {ok, FileInfo} = file:read_file_info(Name),

            %% Read and validate magic tag.
            {ok, <<?FILE_FORMAT, IdxTagLen:integer-unsigned>>} = file:pread(File, 0, 4 + byte_size(<<1/integer-unsigned>>)),
            IdxMgr = idx_type(file:pread(File, 4, IdxTagLen)),

            IdxState = case IdxMgr:init(File, 4 + byte_size(<<1/integer-unsigned>>) +  IdxTagLen) of
                      {ok, IdxState} ->
                          IdxState;
                      _ ->
                          {error, index_mgr_failed_to_init}
% TODO          {error, run_recovery}
% TODO          {error, corrupt}
                  end,

            {ok, #index{file=File, idxmgr=IdxMgr, idxstate=IdxState, name=Name, config=Config}}
    end.

destroy(#index{file=File, name=Name, idxmgr=IdxMgr, idxstate=IdxState}) ->
    ok = IdxMgr:destroy(IdxState),
    ok = file:close(File),
    file:delete(Name).

serialize(#index{file=File}=Index) ->
    {ok, Position} = file:position(File, cur),
    ok = file:close(File),
    {seq_read_file, Index, Position}.

deserialize({seq_read_file, Index, Position}) ->
    {ok, #index{file=File}=Index2} = open(Index#index.name, Index#index.config),
    {ok, Position} = file:position(File, {bof, Position}),
    Index2.


filter(Pred, L) -> lists:reverse(filter(Pred, L,[])).

filter(_, [], Acc) -> Acc;
filter(Pred, [H|T], Acc) ->
    case Pred(H) of
        true  -> filter(Pred, T, [H|Acc]);
        false -> filter(Pred, T, Acc)
    end.

range_fold(Fun, Acc0, #index{file=File,root=Root}, Range) ->
    case lookup_node(File,Range#key_range.from_key,Root,?FIRST_BLOCK_POS) of
        {ok, {Pos,_}} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#key_range.limit);
        {ok, Pos} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#key_range.limit);
        none ->
            {done, Acc0}
    end.

fold_until_stop(Fun,Acc,List) ->
    fold_until_stop2(Fun, {continue, Acc}, List).

fold_until_stop2(_Fun,{stop,Result},_) ->
    {stopped, Result};
fold_until_stop2(_Fun,{continue, Acc},[]) ->
    {ok, Acc};
fold_until_stop2(Fun,{continue, Acc},[H|T]) ->
    fold_until_stop2(Fun,Fun(H,Acc),T).

% TODO this is duplicate code also found in hanoidb_nursery
is_expired(?TOMBSTONE) ->
    false;
is_expired({_Value, TStamp}) ->
    hanoidb_util:has_expired(TStamp);
is_expired(Bin) when is_binary(Bin) ->
    false.

get_value({Value, _TStamp}) ->
    Value;
get_value(Value) ->
    Value.

do_range_fold(Fun, Acc0, File, Range, undefined) ->
    case next_leaf_node(File) of
        eof ->
            {done, Acc0};

        {ok, #node{members=Members}} ->
            case fold_until_stop(fun({Key,_}, Acc) when not ?KEY_IN_TO_RANGE(Key,Range) ->
                                         {stop, {done, Acc}};
                                    ({Key,Value}, Acc) when ?KEY_IN_FROM_RANGE(Key, Range) ->
                                         case is_expired(Value) of
                                             true ->
                                                 {continue, Acc};
                                             false ->
                                                 {continue, Fun(Key, get_value(Value), Acc)}
                                         end;
                                    (_, Acc) ->
                                         {continue, Acc}
                                 end,
                                 Acc0,
                                 Members) of
                {stopped, Result} -> Result;
                {ok, Acc1} ->
                    do_range_fold(Fun, Acc1, File, Range, undefined)
            end
    end;

do_range_fold(Fun, Acc0, File, Range, N0) ->
    case next_leaf_node(File) of
        eof ->
            {done, Acc0};

        {ok, #node{members=Members}} ->
            case fold_until_stop(fun({Key,_}, {0,Acc}) ->
                                         {stop, {limit, Acc, Key}};
                                    ({Key,_}, {_,Acc}) when not ?KEY_IN_TO_RANGE(Key,Range)->
                                         {stop, {done, Acc}};
                                    ({Key,?TOMBSTONE}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
                                         {continue, {N1, Fun(Key, ?TOMBSTONE, Acc)}};
                                    ({Key,{?TOMBSTONE,TStamp}}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
                                         case hanoidb_utils:has_expired(TStamp) of
                                             true ->
                                                 {continue, {N1,Acc}};
                                             false ->
                                                 {continue, {N1, Fun(Key, ?TOMBSTONE, Acc)}}
                                         end;
                                    ({Key,Value}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
                                         case is_expired(Value) of
                                             true ->
                                                 {continue, {N1,Acc}};
                                             false ->
                                                 {continue, {N1-1, Fun(Key, get_value(Value), Acc)}}
                                         end;
                                    (_, Acc) ->
                                         {continue, Acc}
                                 end,
                                 {N0, Acc0},
                                 Members) of
                {stopped, Result} -> Result;
                {ok, {N2, Acc1}} ->
                    do_range_fold(Fun, Acc1, File, Range, N2)
            end
    end.


close(#index{file=File}) ->
    file:close(File).


lookup(#index{file=File, root=Node, bloom=Bloom, config=Config }, Key) ->
    case ebloom:contains(Bloom, Key) of
        true ->
            case lookup_in_node(File,Node,Key) of
                not_found ->
                    not_found;
                {ok, {Value, TStamp}} ?ASSERT_WHEN(Value =:= ?TOMBSTONE; is_binary(Value))  ->
                    case hanoidb_utils:has_expired(TStamp) of
                        true -> not_found;
                        false -> {ok, Value}
                    end;
                {ok, Value}=Reply ?ASSERT_WHEN(Value =:= ?TOMBSTONE; is_binary(Value)) ->
                    Reply
            end;
        false ->
            not_found
    end.

lookup_in_node(_File,#node{level=0,members=Members},Key) ->
    case lists:keyfind(Key,1,Members) of
        false ->
            not_found;
        {_,Value} ->
            {ok, Value}
    end;

lookup_in_node(File,#node{members=Members},Key) ->
    case find_1(Key, Members) of
        {ok, {Pos,Size}} ->
            %% do this in separate process, to avoid having to
            %% garbage collect all the inner node junk
            PID = proc_lib:spawn_link(fun() ->
                                              receive
                                                  ?CALL(From,read) ->
                                                      case read_node(File, {Pos,Size}) of
                                                          {ok, Node} ->
                                                              Result = lookup_in_node2(File, Node, Key),
                                                              plain_rpc:send_reply(From, Result);
                                                          {error, _}=Error ->
                                                              plain_rpc:send_reply(From, Error)
                                                      end
                                              end
                                      end),
            try plain_rpc:call(PID, read)
            catch
                Class:Ex ->
                    error_logger:error_msg("crashX: ~p:~p ~p~n", [Class,Ex,erlang:get_stacktrace()]),
                    not_found
            end;

        not_found ->
            not_found
    end.


lookup_in_node2(_File,#node{level=0,members=Members},Key) ->
    case lists:keyfind(Key,1,Members) of
        false ->
            not_found;
        {_,Value} ->
            {ok, Value}
    end;

lookup_in_node2(File,#node{members=Members},Key) ->
    case find_1(Key, Members) of
        {ok, {Pos,Size}} ->
            case read_node(File, {Pos,Size}) of
                {ok, Node} ->
                    lookup_in_node2(File, Node, Key);
                {error, _}=Error ->
                    Error
            end;
        not_found ->
            not_found
    end.


find_1(K, [{K1,V},{K2,_}|_]) when K >= K1, K < K2 ->
    {ok, V};
find_1(K, [{K1,V}]) when K >= K1 ->
    {ok, V};
find_1(K, [_|T]) ->
    find_1(K,T);
find_1(_, _) ->
    not_found.


find_start(K, [{_,V},{K2,_}|_]) when K < K2 ->
    {ok, V};
find_start(_, [{_,{_,_}=V}]) ->
    {ok, V};
find_start(K, KVs) ->
    find_1(K, KVs).
