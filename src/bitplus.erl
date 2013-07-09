%%%-------------------------------------------------------------------
%%% File:      bitplus.erl
%%% @author    Harish Mallipeddi <harish.mallipeddi@gmail.com> []
%%% @copyright 2009 Harish Mallipeddi
%%% @doc  
%%%
%%% @end  
%%%
%%% @since Mon Jan 19 00:11:50 SGT 2009 by Harish Mallipeddi
%%%-------------------------------------------------------------------
-module(bitplus).
-author('harish.mallipeddi@gmail.com').

-export([compress/1, decompress/1, empty/0, size_compressed/1, size_decompressed/1]).
-export([get/2, set/3, append/2]).
-export([logical_and/2, logical_or/2, logical_not/1]).
%%-compile(export_all).

-record(bitplus, {data}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

compress(Bin1) when is_bitstring(Bin1) -> #bitplus{data=compress_(Bin1)}.

decompress(#bitplus{data=B}) -> decompress_(B).

empty() -> compress(<<>>).

size_compressed(#bitplus{data=B}) -> bit_size(B).

size_decompressed(#bitplus{data=B}) -> size_(B).

%% get the Nth bit
get(#bitplus{data=B}, N) -> get_(B, N).

%% set Nth bit to 0 or 1
set(#bitplus{data=B}, N, SetBit) -> #bitplus{data=set_(B, N, SetBit)}.

%% append AppendBit to the end.
append(#bitplus{data=B}, AppendBit) -> #bitplus{data=append_(B, AppendBit)}.

logical_and(#bitplus{data=B1}, #bitplus{data=B2}) -> #bitplus{data=lAnd(B1, B2)}.

logical_or(#bitplus{data=B1}, #bitplus{data=B2}) -> #bitplus{data=lOr(B1, B2)}.

% TODO
logical_not(#bitplus{data=B}) -> #bitplus{data=lNot(B)}.

%%====================================================================
%% Internal functions
%%====================================================================

append_(B, AppendBit) when is_bitstring(B) -> 
    pack(lists:reverse(
        append_(lists:reverse(decompose(B)), AppendBit)
    ));
append_([{fill, FillBit, N}|RestDWords], AppendBit) ->
    [{literal, 1, <<AppendBit:1>>} | [{fill, FillBit, N} | RestDWords]];
append_([{literal, 31, Literal}|RestDWords], AppendBit) ->
    [{literal, 1, <<AppendBit:1>>} | [{literal, 31, Literal}|RestDWords]];
append_([{literal, 30, Literal}|RestDWords], AppendBit) ->
    <<N:30>> = Literal,
    N1 = (N bsl 1) + AppendBit,
    Literal1 = <<N1:31>>,
    if
        length(RestDWords) > 0 ->            
            [Next|Remaining] = RestDWords,
            Check0 = Literal1 =:= all_zeros31() andalso is_dword_0fill(Next),
            Check1 = Literal1 =:= all_ones31() andalso is_dword_1fill(Next),
            if
                Check0 ->
                    {fill, 0, OldL} = Next,
                    [{fill, 0, OldL+1}|Remaining];
                Check1 ->
                    {fill, 1, OldL} = Next,
                    [{fill, 1, OldL+1}|Remaining];
                true ->
                    [{literal, 31, Literal1}|RestDWords]
            end;
        true ->
            Check0 = Literal1 =:= all_zeros31(),
            Check1 = Literal1 =:= all_ones31(),
            if 
                Check0 -> [{fill, 0, 1}];
                Check1 -> [{fill, 1, 1}];
                true -> [{literal, 31, Literal1}]
            end
    end;
append_([{literal, Length, Literal}|RestDWords], AppendBit) ->
    <<N:Length>> = Literal,
    N1 = (N bsl 1) + AppendBit,
    Length1 = Length + 1,
    Literal1 = <<N1:Length1>>,
    [{literal, Length1, Literal1}|RestDWords];
append_([], AppendBit) ->
    [{literal, 1, <<AppendBit:1>>}].
    
is_dword_0fill({fill, 0, _}) -> true;
is_dword_0fill(_) -> false.
is_dword_1fill({fill, 1, _}) -> true;
is_dword_1fill(_) -> false.

%%====================================================================
%% set & get
%%====================================================================

set_(B, N, 1) -> lOr(B, set_mask(size_(B), N, 1));
set_(B, N, 0) -> lAnd(B, set_mask(size_(B), N, 0)).

%% return a compressed bitstring which can act as the bitmask
%% for setting the Nth bit of some BSize'd bitstring to the value of SetBit.
set_mask(BSize, N, SetBit) ->
    Tot = ceiling(BSize / 31),
    Nth = ceiling(N / 31),
    L1 = case Nth > 1 of 
            true -> [{fill, bit_not(SetBit), Nth-1}]; 
            false -> [] 
    end,
    L2 = case Tot-Nth == 0 of
            true -> [set_mask_literal((BSize-1) rem 31 + 1, N, SetBit)];
            false -> [set_mask_literal(31, N, SetBit), {fill, bit_not(SetBit), Tot-Nth}]
    end,
    pack(L1 ++ L2).

set_mask_literal(Length, N, SetBit) ->
    Idx = (Length-1) - ((N-1) rem Length),
    Mask = case SetBit of
        1 -> 1 bsl Idx;
        0 -> bnot (1 bsl Idx)
    end,
    {literal, Length, <<Mask:Length>>}.

%% get nth bit from bitmap without decompressing
get_(B, N) when is_bitstring(B) -> get_(decompose(B), N);
get_([{fill, FillBit, M}|Rest], N) when N > 0 ->
    case N =< 31*M of
        true -> FillBit;
        false -> get_(Rest, N - 31*M)
    end;
get_([{literal, Length, Literal}|Rest], N) when N > 0 ->
    case N =< Length of
        true ->
            Idx = Length - N,
            Mask = 1 bsl Idx,
            <<L:Length>> = Literal,
            case (L band Mask) > 0 of
                true -> 1;
                false -> 0
            end;
        false -> get_(Rest, N - Length)
    end.

%% compute the size of a compressed bitstring without decompressing it.
size_(B) when is_bitstring(B) -> size_(decompose(B), 0).
size_([{fill, _, N}|Rest], Acc) -> size_(Rest, 31*N + Acc);
size_([{literal, Length, _}|Rest], Acc) -> size_(Rest, Length + Acc);
size_([], Acc) -> Acc.

%%====================================================================
%% Logical Operations - AND, OR, NOT
%%====================================================================

lNot(B) when is_bitstring(B) ->
    B.

lAnd(A, B) when is_bitstring(A) andalso is_bitstring(B) ->
    DWordsC = lAnd(decompose(A), decompose(B), []),
    pack(DWordsC).
lAnd([{fill, 0, N}|RestA], B, Acc) ->
    lAnd(RestA, skipN(N, B), check_prev({fill, 0, N}, Acc));
lAnd([{fill, 1, N}|RestA], B, Acc) ->
    {FirstNofB, RestB} = splitN(N, B),
    [X|Y] = FirstNofB,
    Acc1 = lists:reverse(Y) ++ check_prev(X, Acc),
    lAnd(RestA, RestB, Acc1);
lAnd([{literal, _, _}|RestA], [{fill, 0, N}|RestB], Acc) ->
    B = [{fill, 0, N}|RestB],
    lAnd(RestA, skipN(1, B), check_prev({fill, 0, 1}, Acc));
lAnd([{literal, Length, Literal}|RestA], [{fill, 1, N}|RestB], Acc) ->
    B = [{fill, 1, N}|RestB],
    lAnd(RestA, skipN(1, B), check_prev({literal, Length, Literal}, Acc));
lAnd([{literal, 31, LiteralA}|RestA], [{literal, 31, LiteralB}|RestB], Acc) ->
    <<A:31>> = LiteralA,
    <<B:31>> = LiteralB,
    C = A band B,
    BinC = <<C:31>>,
    Pat0 = all_zeros31(),
    FinalC = case BinC of
        Pat0 -> {fill, 0, 1}; % ANDing resulted in 0-fill word
        _ -> {literal, 31, BinC}
    end,
    lAnd(RestA, RestB, check_prev(FinalC, Acc));
lAnd([{literal, LengthA, LiteralA}|[]], [{literal, LengthB, LiteralB}|[]], Acc) when LengthA == LengthB ->
    <<A:LengthA>> = LiteralA,
    <<B:LengthB>> = LiteralB,
    C = A band B,
    BinC = <<C:LengthA>>,
    lAnd([],[],check_prev({literal, LengthA, BinC}, Acc));
lAnd([],[],Acc) ->
    lists:reverse(Acc).


lOr(A, B) when is_bitstring(A) andalso is_bitstring(B) ->
    DWordsC = lOr(decompose(A), decompose(B), []),
    pack(DWordsC).
lOr([{fill, 1, N}|RestA], B, Acc) ->
    lOr(RestA, skipN(N, B), check_prev({fill, 1, N}, Acc));
lOr([{fill, 0, N}|RestA], B, Acc) ->
    {FirstNofB, RestB} = splitN(N, B),
    [X|Y] = FirstNofB,
    Acc1 = lists:reverse(Y) ++ check_prev(X, Acc),
    lOr(RestA, RestB, Acc1);
lOr([{literal, _, _}|RestA], [{fill, 1, N}|RestB], Acc) ->
    B = [{fill, 1, N}|RestB],
    lOr(RestA, skipN(1, B), check_prev({fill, 1, 1}, Acc));
lOr([{literal, Length, Literal}|RestA], [{fill, 0, N}|RestB], Acc) ->
    B = [{fill, 0, N}|RestB],
    lOr(RestA, skipN(1, B), check_prev({literal, Length, Literal}, Acc));
lOr([{literal, 31, LiteralA}|RestA], [{literal, 31, LiteralB}|RestB], Acc) ->
    <<A:31>> = LiteralA,
    <<B:31>> = LiteralB,
    C = A bor B,
    BinC = <<C:31>>,
    Pat1 = all_ones31(),
    FinalC = case BinC of
        Pat1 -> {fill, 1, 1}; % ANDing resulted in 1-fill word
        _ -> {literal, 31, BinC}
    end,
    lOr(RestA, RestB, check_prev(FinalC, Acc));
lOr([{literal, LengthA, LiteralA}|[]], [{literal, LengthB, LiteralB}|[]], Acc) when LengthA == LengthB ->
    <<A:LengthA>> = LiteralA,
    <<B:LengthB>> = LiteralB,
    C = A bor B,
    BinC = <<C:LengthA>>,
    lOr([],[],check_prev({literal, LengthA, BinC}, Acc));
lOr([],[],Acc) ->
    lists:reverse(Acc).


%% perform CONS operation aka [H|T] after checking for 
%% the possibility of 2 similar fill words being adjacent two each other.
%% If yes, modify the first elem of T and return the new list.
%% If no, just do plain old CONS.
check_prev({fill, FillBit, N}, [{fill, FillBit, M}|T]) -> [{fill, FillBit, N+M}|T];
check_prev(H, T) -> [H|T].


%% split DWords list into {FirstN, Rest}
splitN(N, DWords) ->
    {FirstN, Rest} = splitN(N, DWords, []),
    {lists:reverse(FirstN), Rest}.
splitN(0, RestDWords, Acc) -> {Acc, RestDWords};
splitN(N, [{fill, FillBit, M}|RestDWords], Acc) when N < M -> 
    {[{fill, FillBit, N}|Acc], [{fill, FillBit, M-N}|RestDWords]};
splitN(N, [{fill, FillBit, M}|RestDWords], Acc) when N == M ->
    {[{fill, FillBit, M}|Acc], RestDWords};
splitN(N, [{fill, FillBit, M}|RestDWords], Acc) when N > M ->
    splitN(N-M, RestDWords, [{fill, FillBit, M}|Acc]);
splitN(N, [{literal, Length, Literal}|RestDWords], Acc) ->
    splitN(N-1, RestDWords, [{literal, Length, Literal}|Acc]).

%% skip N words in the DWords list and return the remaining list
skipN(0, DWords) -> DWords;
skipN(N, [{fill, FillBit, M}|RestDWords]) when N < M -> [{fill, FillBit, M-N}|RestDWords];
skipN(N, [{fill, _FillBit, M}|RestDWords]) when N == M -> RestDWords;
skipN(N, [{fill, _FillBit, M}|RestDWords]) when N > M -> skipN(N-M, RestDWords);
skipN(N, [{literal, _Length, _Literal}|RestDWords]) -> skipN(N-1, RestDWords).

%%====================================================================
%% Decompression
%%====================================================================

decompress_(B) ->
    DWords = decompose(B),
    Bins = lists:reverse(decompress_(DWords, [])),
    list_to_bitstring(Bins). % join the list of 31-bit bitstrings into 1 long bitstring. 
decompress_([{fill, 0, N}|Rest], Acc) -> decompress_(Rest, replicate(all_zeros31(), N) ++ Acc);
decompress_([{fill, 1, N}|Rest], Acc) -> decompress_(Rest, replicate(all_ones31(), N) ++ Acc);
decompress_([{literal, _Length, Literal}|Rest], Acc) -> decompress_(Rest, [Literal|Acc]);
decompress_([], Acc) -> Acc.

%% split a compressed bitstring (bitplus) into words.
decompose(B) ->
    lists:reverse(decompose(B, [])).
decompose(B, Acc) when bit_size(B) > 64 -> % not the last 2 words - (fill words + literal words)
    RestSize = bit_size(B) - 32,
    <<G:32, Rest:RestSize>> = B,
    G1 = case <<G:32>> of
            <<2#10:2, N:30>> -> {fill, 0, N};
            <<2#11:2, N:30>> -> {fill, 1, N};
            <<2#0:1, Literal:31>> -> {literal, 31, <<Literal:31>>}
    end,
    decompose(<<Rest:RestSize>>, [G1|Acc]);
decompose(B, Acc) when bit_size(B) == 64 -> % last 2 words - (active word + mask word)
    <<A:32, M:32>> = B,
    G1 = case <<M:32>> of
        <<32:32>> -> % all 32 bits of active word are meaningful
            case <<A:32>> of
                <<2#10:2, N:30>> -> {fill, 0, N};
                <<2#11:2, N:30>> -> {fill, 1, N};
                <<2#0:1, Literal:31>> -> {literal, 31, <<Literal:31>>}
            end;
        <<N:32>> -> % only N bits of active word are meaningful
            {literal, N, <<A:N>>}
    end,
    [G1|Acc].

%% Pack DWords back into the original compressed bitstring.
%% Opposite of decompose/1.
pack(DWords) -> list_to_bitstring(lists:reverse(pack(DWords, []))).
pack([{fill, 0, N}|RestDWords], Acc) -> pack(RestDWords, [<<2#10:2, N:30>>|Acc]);
pack([{fill, 1, N}|RestDWords], Acc) -> pack(RestDWords, [<<2#11:2, N:30>>|Acc]);
pack([{literal, 31, <<Literal:31>>}|RestDWords], Acc) -> pack(RestDWords, [<<2#0:1, Literal:31>>|Acc]);
pack([{literal, Length, Literal}|[]], Acc) ->
    <<A:Length>> = Literal,
    [<<Length:32>>|[<<A:32>>|Acc]]; % [Mask|[Active|Acc]]
pack([], Acc) -> [<<32:32>>|Acc].

%% replicate supplied W n times in a list & return the list.
replicate(W, N) -> replicate(W, N, []).
replicate(_W, 0, Acc) -> Acc;
replicate(W, N, Acc) -> replicate(W, N-1, [W|Acc]).

%%====================================================================
%% Compression
%%====================================================================

compress_(Bin) ->
    L = split31(Bin),
    Words = lists:reverse(compress_(L, [])),
    list_to_bitstring(Words). %  join list of words into 1 long bitstring
compress_([H|Rest], Acc) when bit_size(H) == 31 ->
    Pat1 = all_ones31(),
    Pat0 = all_zeros31(),
    case H of
        Pat1 -> % 1-fill word
            {Remaining, Count} = check_consecutive(Pat1, Rest, 0),
            compress_(Remaining, [fill_word_1(1+Count)|Acc]);
        Pat0 -> % 0-fill word
            {Remaining, Count} = check_consecutive(Pat0, Rest, 0),
            compress_(Remaining, [fill_word_0(1+Count)|Acc]);
        _ -> % literal word
            compress_(Rest, [literal_word(H)|Acc])
    end;
compress_([H|_Rest], Acc) when bit_size(H) < 31 ->
    %% "active word" (last word) is just a word which has <31 useful bits.
    %% active word is followed by another word (lets call it "mask word") which stores
    %% an integer representing the no. of useful bits in the active word.
    [mask_word(bit_size(H))|[active_word(H)|Acc]];
compress_([], Acc) ->
    [mask_word(32)|Acc].

active_word(H) ->
    HSize = bit_size(H),
    <<H1:HSize>> = H,
    <<H1:32>>.

mask_word(HSize) -> <<HSize:32>>.

%% literal words always begin with 0 followed by the 31-bits provided as-is (hence the name literal).
literal_word(<<H:31>>) -> <<2#0:1, H:31>>.

%% fill words always begin with 1 followed by the fill-bit (1 or 0)
fill_word_0(N) -> <<2#10:2, N:30>>. % generate a 0-fill word representing N 31-bit 0 bitstrings
fill_word_1(N) -> <<2#11:2, N:30>>.

%% split given bitstring into 31-bit bitstrings.
split31(Bin) ->
    lists:reverse(split31(Bin, [])).
split31(Bin, Acc) when bit_size(Bin) > 31 ->
    RestSize = bit_size(Bin) - 31,
    <<G:31,Rest:RestSize>> = Bin,
    split31(<<Rest:RestSize>>, [<<G:31>>|Acc]);
split31(Bin, Acc) when bit_size(Bin) =< 31 ->
    [Bin|Acc].

check_consecutive(Pattern, [H|Rest], Count) ->
    case H of
        Pattern ->
            check_consecutive(Pattern, Rest, Count+1);
        _ ->
            {[H|Rest], Count}
    end;
check_consecutive(_, [], Count) -> {[], Count}.


%% Utils

all_ones31() -> <<2#1111111111111111111111111111111:31>>.
all_zeros31() -> <<2#0000000000000000000000000000000:31>>.

%%is_all_ones31(<<2#1111111111111111111111111111111:31>>) -> true;
%%is_all_ones31(_) -> false.
%%is_all_zeros31(<<2#0000000000000000000000000000000:31>>) -> true;
%%is_all_zeros31(_) -> false.

ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.
bit_not(0) -> 1;
bit_not(1) -> 0.

%%====================================================================
%% Tests for internal methods
%%====================================================================
-ifdef(TEST).

bit_not_test() ->
    1 = bit_not(0),
    0 = bit_not(1).

empty_test() ->
    Empty = bitplus:empty(),
    #bitplus{data=_E} = Empty,
    64 = bitplus:size_compressed(Empty),
    0 = bitplus:size_decompressed(Empty).

compress_decompress_test() ->
    % Input: 128-bit vector - 1,20*0,3*1,79*0,25*1 
    BinA = <<2#10000000000000000000011100000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111:128>>,
    BA = bitplus:compress(BinA),
    BinA = bitplus:decompress(BA).

and_test() ->
    % Input: two 128-bit vectors
    BinA = <<16#40000380:31, 16#00000000:31, 16#00000000:31, 16#001FFFFF:31, 16#F:4>>,
    BinB = <<16#7FFFFFFF:31, 16#7FFFFFFF:31, 16#7C0001E0:31, 16#3FE00000:31, 16#3:4>>,
    % Expected output: 128-bit vector
    BinC = <<16#40000380:31, 16#00000000:31, 16#00000000:31, 16#00000000:31, 16#3:4>>,
    
    BA = bitplus:compress(BinA),
    BB = bitplus:compress(BinB),
    BC = bitplus:logical_and(BA, BB),
    BinC = bitplus:decompress(BC).

or_test() ->
    % Input: two 128-bit vectors
    BinA = <<16#40000380:31, 16#00000000:31, 16#00000000:31, 16#001FFFFF:31, 16#F:4>>,
    BinB = <<16#7FFFFFFF:31, 16#7FFFFFFF:31, 16#7C0001E0:31, 16#3FE00000:31, 16#3:4>>,
    % Expected output: 128-bit vector
    BinC = <<16#7FFFFFFF:31, 16#7FFFFFFF:31, 16#7C0001E0:31, 16#3FFFFFFF:31, 16#F:4>>,
    
    BA = bitplus:compress(BinA),
    BB = bitplus:compress(BinB),
    BC = bitplus:logical_or(BA, BB),
    BinC = bitplus:decompress(BC).

get_test() ->
    Bin = <<2#10000000000000000000011100000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111:128>>,
    B = bitplus:compress(Bin),
    1 = bitplus:get(B, 1), % get the 1st bit of bitvector
    0 = bitplus:get(B, 2),
    0 = bitplus:get(B, 75),
    1 = bitplus:get(B, 128),
    ?assertError(function_clause, bitplus:get(B, 1000)). % there's no 1000th bit.

set_test() ->
    Bin = <<2#10000000000000000000011100000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111:128>>,
    Bin1 = <<2#10000000000000000000011100000000000001000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111:128>>, % Bin1 = changed 38th bit of Bin to 1
    B = bitplus:compress(Bin),
    B1 = bitplus:set(B, 38, 1), % set 38th bit of B to 1
    Bin1 = bitplus:decompress(B1),
    B = bitplus:set(B1, 38, 0). % set it back

append_test() ->
    Bin = <<2#10000000000000000000011100000000000000000000000000000000000000000000000000000000000000000000000000000001111111111111111111111111:128>>,
    Bin1 = <<2#100000000000000000000111000000000000000000000000000000000000000000000000000000000000000000000000000000011111111111111111111111110:129>>,
    Bin2 = <<2#1000000000000000000001110000000000000000000000000000000000000000000000000000000000000000000000000000000111111111111111111111111101:130>>,
    B = bitplus:compress(Bin),
    B1 = bitplus:append(B, 0),
    Bin1 = bitplus:decompress(B1),
    B2 = bitplus:append(B1, 1),
    Bin2 = bitplus:decompress(B2).

append_to_empty_test() ->
    Empty = bitplus:empty(),
    B1 = bitplus:append(Empty, 0),
    1 = bitplus:size_decompressed(B1),
    <<2#0:1>> = bitplus:decompress(B1),
    <<2#01:2>> = bitplus:decompress(bitplus:append(B1, 1)).

large_vector_test() ->
    B = random_bitmap(5000),
    5000 = bitplus:size_decompressed(B),
    B = bitplus:logical_and(B, B),
    B = bitplus:logical_or(B, B).

random_bitmap(N) ->
    random_bitmap(N, bitplus:empty()).
random_bitmap(0, B) -> B;
random_bitmap(N, B) ->
    B1 = bitplus:append(B, round(random:uniform())),
    random_bitmap(N-1, B1).

-endif.
