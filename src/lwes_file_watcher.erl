-module (lwes_file_watcher).

%% API
-export ([ scan/1,
           changes/1,
           changes/2
         ]).

% The goal of this modules is to watch directories and files, and alert
% interested parties about when things change.
%
% The usage is as follows
%
% 1. get a scan right now
%
%    Scan = scan (["/tmp"]),
%
% 2. see what changed from an empty scan
%
%    changes (Scan)
%
% 3. save a scan then see what changed the next time
%
%    changes (OldScan, NewScan)
%
% Changes will return a list of the form
%
% [ {dir, added, "dir"},
%   {dir, removed, "dir"},
%   {file, added, "file"},
%   {file, removed, "file"},
%   {file, changed, "file"}
% ]

%%====================================================================
%% API
%%====================================================================
scan (FilePaths) ->
  scan_filepaths (FilePaths).

changes (Scan) ->
  file_changes ([], Scan).

changes (OldScan, NewScan) ->
  file_changes (OldScan, NewScan).

%%====================================================================
%% Internal
%%====================================================================
scan_filepaths (L) ->
  ordsets:from_list (lists:flatten (scan_filepaths (L, []))).

scan_filepaths ([First = [_|_]|Rest], Accum) ->
  scan_filepaths (Rest, [ scan_filepath (First) | Accum ]);
scan_filepaths ([First], Accum) ->
  [ scan_filepath (First) | Accum ];
scan_filepaths (First, Accum) ->
  [ scan_filepath (First) | Accum ].

% non-tail recursively scan directory/file and return whether it is a
% file, a directory or missing
scan_filepath ([]) ->
  [];
scan_filepath (FilePath) ->
  case filelib:is_dir (FilePath) of
    true ->
      case file:list_dir (FilePath) of
        {ok, Files} ->
          [ { dir, FilePath } |
            lists:foldl (
              fun (F, A) ->
                FP = filename:join ([FilePath, F]),
                [ scan_filepath (FP) | A ]
              end,
              [],
              Files
            )
          ];
        { error, _ } ->
          []
      end;
    false ->
      case filelib:is_regular (FilePath) of
        true ->
          [ { file, FilePath, filelib:last_modified (FilePath) } ];
        false ->
          [ ]
      end
  end.

file_changes (OldScan, NewScan) ->
  file_changes (OldScan, NewScan, []).

file_changes ([], [], Accum) ->
  lists:reverse (Accum);
file_changes (OL = [], [{dir, D} | NR], Accum) ->
  file_changes (OL, NR, [ {dir, added, D} | Accum ]);
file_changes (OL = [], [{file, F, _} | NR], Accum) ->
  file_changes (OL, NR, [ {file, added, F} | Accum ]);
file_changes ([{dir, D} | OR], NL = [], Accum) ->
  file_changes (OR, NL, [ {dir, removed, D} | Accum ]);
file_changes ([{file, F, _} | OR], NL = [], Accum) ->
  file_changes (OR, NL, [ {file, removed, F} | Accum ]);
file_changes ([O|OR], [N|NR], Accum) when O =:= N ->
  file_changes (OR, NR, Accum);
file_changes ([{file, FP, OLM}|OR], [{file, FP, NLM}|NR], Accum)
  when OLM =/= NLM ->
    file_changes (OR, NR, [ {file, changed, FP} | Accum ]);
file_changes ([O = {file, F, _}|OR], NF = [N|_], Accum) when O < N ->
  file_changes (OR, NF, [ {file, removed, F} | Accum ]);
file_changes ([O = {dir, D}|OR], NF = [N|_], Accum) when O < N ->
  file_changes (OR, NF, [ {dir, removed, D} | Accum ]);
file_changes (OF = [O|_], [N = {file, F, _}|NR], Accum) when O > N ->
  file_changes (OF, NR, [ {file, added, F} | Accum ]);
file_changes (OF = [O|_], [N = {dir, D}|NR], Accum) when O > N ->
  file_changes (OF, NR, [ {dir, added, D} | Accum ]).

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(TEST).
-include_lib ("eunit/include/eunit.hrl").

% write tests here :)

-endif.
