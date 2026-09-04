def aggregates:
  [
    {field: "AdvEngineID"},
    {field: "IsRefresh"},
    {field: "ResolutionWidth"},
    {field: "ResolutionHeight"},
    {field: "ResolutionDepth"},
    {field: "FlashMinor"},
    {field: "NetMajor"},
    {field: "FetchTiming"}
  ]
  | map(
      . as $metric
      | ["SUM", "MIN", "MAX", "COUNT_FIELD"][] as $function
      | {
          function: $function,
          field: $metric.field,
          alias: (
            (if $function == "COUNT_FIELD" then "count" else ($function | ascii_downcase) end)
            + "_" + $metric.field
          )
        }
    );

{
  descriptor_version: 1,
  group_keys: [
    {
      name: "event_bucket",
      column_type: "TIMESTAMP",
      source_column: "EventTime",
      span_interval_ms: 300000
    },
    {name: "URL", column_type: "KEYWORD"},
    {name: "CounterID", column_type: "LONG"}
  ],
  aggregates: aggregates
}
