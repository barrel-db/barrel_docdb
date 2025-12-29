%%%-------------------------------------------------------------------
%%% @doc Changes feed API for barrel_docdb
%%%
%%% Provides functions to track and query document changes in a
%%% database. Changes are ordered by sequence number.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes).

-include("barrel_docdb.hrl").

%% API
-export([
    fold_changes/5,
    get_changes/4,
    get_last_seq/2,
    count_changes_since/3
]).

%% Internal - for use by barrel_db_writer
-export([
    write_change/4,
    delete_old_seq/4
]).

%%====================================================================
%% Types
%%====================================================================

-type changes_result() :: #{
    changes := [change()],
    last_seq := seq(),
    pending := non_neg_integer()
}.

-type fold_fun() :: fun((change(), Acc :: term()) ->
    {ok, Acc :: term()} | {stop, Acc :: term()} | stop).

-type changes_opts() :: #{
    include_docs => boolean(),
    limit => non_neg_integer(),
    descending => boolean(),
    style => main_only | all_docs,
    doc_ids => [docid()]
}.

-export_type([changes_result/0, fold_fun/0, changes_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Fold over changes since a given sequence (exclusive)
%% Changes returned are strictly after the given sequence.
%% Use 'first' to get all changes from the beginning.
-spec fold_changes(barrel_store_rocksdb:db_ref(), db_name(), seq() | first, fold_fun(), term()) ->
    {ok, term(), seq()}.
fold_changes(StoreRef, DbName, Since, Fun, Acc) ->
    {StartSeq, StartKey} = case Since of
        first ->
            %% Start from the very beginning
            Min = barrel_sequence:min_seq(),
            {Min, barrel_store_keys:doc_seq(DbName, Min)};
        SinceSeq ->
            %% Exclusive: changes after this sequence
            Next = barrel_sequence:inc(SinceSeq),
            {SinceSeq, barrel_store_keys:doc_seq(DbName, Next)}
    end,
    EndKey = barrel_store_keys:doc_seq_end(DbName),

    {LastSeq, FinalAcc} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, Value, {CurrentSeq, AccIn}) ->
            ChangeSeq = barrel_store_keys:decode_seq_key(DbName, Key),
            Change = decode_change(Value, ChangeSeq),
            case Fun(Change, AccIn) of
                {ok, AccOut} ->
                    {ok, {ChangeSeq, AccOut}};
                {stop, AccOut} ->
                    {stop, {ChangeSeq, AccOut}};
                stop ->
                    {stop, {CurrentSeq, AccIn}}
            end
        end,
        {StartSeq, Acc}
    ),
    {ok, FinalAcc, LastSeq}.

%% @doc Get a list of changes since a sequence
-spec get_changes(barrel_store_rocksdb:db_ref(), db_name(), seq() | first, changes_opts()) ->
    {ok, [change()], seq()}.
get_changes(StoreRef, DbName, Since, Opts) ->
    Limit = maps:get(limit, Opts, infinity),
    DocIds = maps:get(doc_ids, Opts, undefined),
    Style = maps:get(style, Opts, all_docs),

    FoldFun = fun(Change, {Count, Changes}) ->
        Include = case DocIds of
            undefined -> true;
            Ids when is_list(Ids) -> lists:member(maps:get(id, Change), Ids)
        end,

        case Include of
            false ->
                {ok, {Count, Changes}};
            true ->
                FilteredChange = case Style of
                    main_only ->
                        Change#{changes => [hd(maps:get(changes, Change))]};
                    all_docs ->
                        Change
                end,
                NewCount = Count + 1,
                NewChanges = [FilteredChange | Changes],
                case Limit of
                    infinity ->
                        {ok, {NewCount, NewChanges}};
                    N when NewCount >= N ->
                        {stop, {NewCount, NewChanges}};
                    _ ->
                        {ok, {NewCount, NewChanges}}
                end
        end
    end,

    {ok, {_Count, RevChanges}, LastSeq} = fold_changes(StoreRef, DbName, Since, FoldFun, {0, []}),

    Changes = case maps:get(descending, Opts, false) of
        true -> RevChanges;
        false -> lists:reverse(RevChanges)
    end,

    {ok, Changes, LastSeq}.

%% @doc Get the last sequence number for a database
-spec get_last_seq(barrel_store_rocksdb:db_ref(), db_name()) -> seq().
get_last_seq(StoreRef, DbName) ->
    StartKey = barrel_store_keys:doc_seq_prefix(DbName),
    EndKey = barrel_store_keys:doc_seq_end(DbName),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, _Value, _Acc) ->
            Seq = barrel_store_keys:decode_seq_key(DbName, Key),
            {ok, Seq}
        end,
        barrel_sequence:min_seq()
    ).

%% @doc Count changes since a given sequence (exclusive)
-spec count_changes_since(barrel_store_rocksdb:db_ref(), db_name(), seq()) -> non_neg_integer().
count_changes_since(StoreRef, DbName, Since) ->
    %% Count changes after the given sequence
    StartKey = barrel_store_keys:doc_seq(DbName, barrel_sequence:inc(Since)),
    EndKey = barrel_store_keys:doc_seq_end(DbName),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(_Key, _Value, Count) -> {ok, Count + 1} end,
        0
    ).

%%====================================================================
%% Internal API - for barrel_db_writer
%%====================================================================

%% @doc Write a change entry for a document
-spec write_change(barrel_store_rocksdb:db_ref(), db_name(), seq(), doc_info()) -> ok.
write_change(StoreRef, DbName, Seq, DocInfo) ->
    Key = barrel_store_keys:doc_seq(DbName, Seq),
    Value = encode_change(DocInfo),
    barrel_store_rocksdb:put(StoreRef, Key, Value).

%% @doc Delete an old sequence entry (when document is updated)
-spec delete_old_seq(barrel_store_rocksdb:db_ref(), db_name(), seq(), docid()) -> ok.
delete_old_seq(StoreRef, DbName, OldSeq, _DocId) ->
    Key = barrel_store_keys:doc_seq(DbName, OldSeq),
    barrel_store_rocksdb:delete(StoreRef, Key).

%%====================================================================
%% Internal Functions
%%====================================================================

encode_change(DocInfo) ->
    term_to_binary(DocInfo).

decode_change(Value, Seq) ->
    DocInfo = binary_to_term(Value),
    Rev = maps:get(rev, DocInfo),
    Deleted = maps:get(deleted, DocInfo, false),

    ConflictRevs = get_conflict_revs(DocInfo),
    AllRevs = [#{rev => Rev} | [#{rev => R} || R <- ConflictRevs]],

    Change = #{
        id => maps:get(id, DocInfo),
        seq => Seq,
        rev => Rev,
        changes => AllRevs
    },

    %% Include document body if present
    Change1 = case maps:get(doc, DocInfo, undefined) of
        undefined -> Change;
        Doc -> Change#{doc => Doc}
    end,

    case Deleted of
        true -> Change1#{deleted => true};
        false -> Change1
    end.

get_conflict_revs(#{revtree := RevTree}) when is_map(RevTree) ->
    case barrel_revtree:conflicts(RevTree) of
        [] -> [];
        Conflicts -> [maps:get(id, C) || C <- Conflicts]
    end;
get_conflict_revs(_) ->
    [].
