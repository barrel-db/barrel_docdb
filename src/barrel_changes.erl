%%%-------------------------------------------------------------------
%%% @doc Changes feed API for barrel_docdb
%%%
%%% Provides functions to track and query document changes in a
%%% database. Changes are ordered by HLC (Hybrid Logical Clock)
%%% timestamps for distributed ordering.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes).

-include("barrel_docdb.hrl").

%% API
-export([
    fold_changes/5,
    get_changes/4,
    get_last_seq/2,  %% Returns opaque sequence (encoded HLC binary)
    get_last_hlc/2,  %% Returns decoded HLC timestamp
    count_changes_since/3
]).

%% Internal - for use by barrel_db_writer
-export([
    write_change/4,
    write_change_ops/3,
    delete_old_change/4
]).

%%====================================================================
%% Types
%%====================================================================

-type changes_result() :: #{
    changes := [change()],
    last_hlc := barrel_hlc:timestamp(),
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

%% @doc Fold over changes since a given HLC timestamp (exclusive)
%% Changes returned are strictly after the given HLC.
%% Use 'first' to get all changes from the beginning.
-spec fold_changes(barrel_store_rocksdb:db_ref(), db_name(),
                   barrel_hlc:timestamp() | first, fold_fun(), term()) ->
    {ok, term(), barrel_hlc:timestamp()}.
fold_changes(StoreRef, DbName, Since, Fun, Acc) ->
    {StartHlc, StartKey} = case Since of
        first ->
            %% Start from the very beginning
            Min = barrel_hlc:min(),
            {Min, barrel_store_keys:doc_hlc(DbName, Min)};
        SinceHlc ->
            %% Exclusive: start at SinceHlc, we'll skip matching entries
            {SinceHlc, barrel_store_keys:doc_hlc(DbName, SinceHlc)}
    end,
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    {LastHlc, FinalAcc} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, Value, {CurrentHlc, AccIn}) ->
            ChangeHlc = barrel_store_keys:decode_hlc_key(DbName, Key),
            %% Skip if we're at the exact Since HLC (exclusive)
            case Since =/= first andalso barrel_hlc:equal(ChangeHlc, Since) of
                true ->
                    {ok, {CurrentHlc, AccIn}};
                false ->
                    Change = decode_change(Value, ChangeHlc),
                    case Fun(Change, AccIn) of
                        {ok, AccOut} ->
                            {ok, {ChangeHlc, AccOut}};
                        {stop, AccOut} ->
                            {stop, {ChangeHlc, AccOut}};
                        stop ->
                            {stop, {CurrentHlc, AccIn}}
                    end
            end
        end,
        {StartHlc, Acc}
    ),
    {ok, FinalAcc, LastHlc}.

%% @doc Get a list of changes since an HLC timestamp
-spec get_changes(barrel_store_rocksdb:db_ref(), db_name(),
                  barrel_hlc:timestamp() | first, changes_opts()) ->
    {ok, [change()], barrel_hlc:timestamp()}.
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

    {ok, {_Count, RevChanges}, LastHlc} = fold_changes(StoreRef, DbName, Since, FoldFun, {0, []}),

    Changes = case maps:get(descending, Opts, false) of
        true -> RevChanges;
        false -> lists:reverse(RevChanges)
    end,

    {ok, Changes, LastHlc}.

%% @doc Get the last sequence (opaque encoded HLC) for a database
%% The sequence is an opaque binary that can be used for ordering.
-spec get_last_seq(barrel_store_rocksdb:db_ref(), db_name()) -> binary().
get_last_seq(StoreRef, DbName) ->
    Hlc = get_last_hlc(StoreRef, DbName),
    barrel_hlc:encode(Hlc).

%% @doc Get the last HLC timestamp for a database
-spec get_last_hlc(barrel_store_rocksdb:db_ref(), db_name()) -> barrel_hlc:timestamp().
get_last_hlc(StoreRef, DbName) ->
    StartKey = barrel_store_keys:doc_hlc_prefix(DbName),
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, _Value, _Acc) ->
            Hlc = barrel_store_keys:decode_hlc_key(DbName, Key),
            {ok, Hlc}
        end,
        barrel_hlc:min()
    ).

%% @doc Count changes since a given HLC timestamp (exclusive)
-spec count_changes_since(barrel_store_rocksdb:db_ref(), db_name(),
                          barrel_hlc:timestamp()) -> non_neg_integer().
count_changes_since(StoreRef, DbName, Since) ->
    StartKey = barrel_store_keys:doc_hlc(DbName, Since),
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, _Value, Count) ->
            ChangeHlc = barrel_store_keys:decode_hlc_key(DbName, Key),
            %% Skip if we're at the exact Since HLC (exclusive)
            case barrel_hlc:equal(ChangeHlc, Since) of
                true -> {ok, Count};
                false -> {ok, Count + 1}
            end
        end,
        0
    ).

%%====================================================================
%% Internal API - for barrel_db_writer
%%====================================================================

%% @doc Write a change entry for a document
-spec write_change(barrel_store_rocksdb:db_ref(), db_name(),
                   barrel_hlc:timestamp(), doc_info()) -> ok.
write_change(StoreRef, DbName, Hlc, DocInfo) ->
    [{put, Key, Value}] = write_change_ops(DbName, Hlc, DocInfo),
    barrel_store_rocksdb:put(StoreRef, Key, Value).

%% @doc Return batch operation to write a change entry.
%% Use this to combine with other operations in a single write_batch.
-spec write_change_ops(db_name(), barrel_hlc:timestamp(), doc_info()) ->
    [{put, binary(), binary()}].
write_change_ops(DbName, Hlc, DocInfo) ->
    Key = barrel_store_keys:doc_hlc(DbName, Hlc),
    Value = encode_change(DocInfo),
    [{put, Key, Value}].

%% @doc Delete an old HLC entry (when document is updated)
-spec delete_old_change(barrel_store_rocksdb:db_ref(), db_name(),
                        barrel_hlc:timestamp(), docid()) -> ok.
delete_old_change(StoreRef, DbName, OldHlc, _DocId) ->
    Key = barrel_store_keys:doc_hlc(DbName, OldHlc),
    barrel_store_rocksdb:delete(StoreRef, Key).

%%====================================================================
%% Internal Functions
%%====================================================================

encode_change(DocInfo) ->
    term_to_binary(DocInfo).

decode_change(Value, Hlc) ->
    DocInfo = binary_to_term(Value),
    Rev = maps:get(rev, DocInfo),
    Deleted = maps:get(deleted, DocInfo, false),

    ConflictRevs = get_conflict_revs(DocInfo),
    AllRevs = [#{rev => Rev} | [#{rev => R} || R <- ConflictRevs]],

    Change = #{
        id => maps:get(id, DocInfo),
        hlc => Hlc,
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
