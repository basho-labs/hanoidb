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


-module(hanoidb_idx_btree).
-author('Greg Burd <greg@basho.com>').

%%
%% Streaming btree index file reader/writer.
%%

-include("include/hanoidb.hrl").
-include("src/hanoidb.hrl").

-behaviour(hanoidb_idx_reader).
-behaviour(hanoidb_idx_writer).

-export(init/2, destroy/1, fold/3).

-record(state, {file, root, bloom, start}).

init(File, StartingPos) ->
    %% Read root position, bloom size and bloom filter information
    {ok, <<RootPos:64/integer-unsigned>>} = file:pread(File, FileInfo#file_info.size - 8, 8),
    {ok, <<BloomSize:32/integer-unsigned>>} = file:pread(File, FileInfo#file_info.size - 12, 4),
    {ok, BloomData} = file:pread(File, FileInfo#file_info.size - 12 - BloomSize ,BloomSize),
    {ok, Bloom} = ebloom:deserialize(zlib:unzip(BloomData)),

    %% Read the root node
    {ok, Root} = read_node(File, RootPos),

    {ok, #state{file=File, start=StartingPos, bloom=Bloom}}.

fold(Fun, Acc0, #state{file=File}) ->
    {ok, Node} = read_node(File,?FIRST_BLOCK_POS),
    fold0(File,fun({K,V},Acc) -> Fun(K,V,Acc) end,Node,Acc0).

% @internal
fold0(File,Fun,#node{level=0,members=List},Acc0) ->
    Acc1 = lists:foldl(Fun,Acc0,List),
    fold1(File,Fun,Acc1);
fold0(File,Fun,_InnerNode,Acc0) ->
    fold1(File,Fun,Acc0).

% @internal
fold1(File,Fun,Acc0) ->
    case next_leaf_node(File) of
        eof ->
            Acc0;
        {ok, Node} ->
            fold0(File,Fun,Node,Acc0)
    end.

range_fold(Fun, Acc0, #state{file=File,root=Root,start=FirstBlockPos}, Range, Filter) ->
    case lookup_node(File,Range#key_range.from_key,Root,FirstBlockPos) of
        {ok, {Pos,_}} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#key_range.limit, Filter);
        {ok, Pos} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#key_range.limit, Filter);
        none ->
            {done, Acc0}
    end.

%% @internal
read_node(File,{Pos,Size}) ->
    lager:debug("read_node ~p ~p ~p~n", [File, Pos, Size]),
    {ok, <<_:32, Level:16/unsigned, Data/binary>>} = file:pread(File, Pos, Size),
    hanoidb_util:decode_index_node(Level, Data);

read_node(File,Pos) ->
    {ok, Pos} = file:position(File, Pos),
    Result = read_node(File),
    lager:debug("decoded ~p ~p~n", [Pos, Result]),
    Result.

read_node(File) ->
    {ok, <<Len:32, Level:16/unsigned>>} = file:read(File, 6),
    case Len of
        0 -> eof;
        _ ->
            {ok, Data} = file:read(File, Len-2),
            hanoidb_util:decode_index_node(Level, Data)
    end.

% internal
lookup_node(_File,_FromKey,#node{level=0},Pos) ->
    {ok, Pos};
lookup_node(File,FromKey,#node{members=Members,level=N},_) ->
    case find_start(FromKey, Members) of
        {ok, ChildPos} when N==1 ->
            {ok, ChildPos};
        {ok, ChildPos} ->
            case read_node(File,ChildPos) of
                {ok, ChildNode} ->
                    lookup_node(File,FromKey,ChildNode,ChildPos);
                eof ->
                    none
            end;
        not_found ->
            none
    end.

first_node(#state{file=File, start=FirstBlockPos}) ->
    case read_node(File, FirstBlockStart) of
        {ok, #node{level=0, members=Members}} ->
            {node, Members}
    end.

next_node(#index{file=File}=_Index) ->
    case next_leaf_node(File) of
        {ok, #node{level=0, members=Members}} ->
            {node, Members};
%        {ok, #node{level=N}} when N>0 ->
%            next_node(Index);
        eof ->
            end_of_data
    end.

next_leaf_node(File) ->
    case file:read(File, 6) of
        eof ->
            %% premature end-of-file
            eof;
        {ok, <<0:32, _:16>>} ->
            eof;
        {ok, <<Len:32, 0:16>>} ->
            {ok, Data} = file:read(File, Len-2),
            hanoidb_util:decode_index_node(0, Data);
        {ok, <<Len:32, _:16>>} ->
            {ok, _} = file:position(File, {cur,Len-2}),
            next_leaf_node(File)
    end.
