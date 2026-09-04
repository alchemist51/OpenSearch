# ClickBench MV setup

Each numbered script performs exactly one `curl` request. Defaults:

- OpenSearch: `http://localhost:9200`
- Source index: `cb100m`
- View and target index: `cb100m_mv`
- Definition: `span(EventTime, 5m)`, `URL`, `CounterID`; SUM/MIN/MAX/COUNT_FIELD over eight metrics

Run in order:

```bash
cd /home/ec2-user/OpenSearch/sandbox/plugins/mv-data-format/scripts/clickbench
./01-create-source-index.sh
./02-validate-mv-definition.sh
./03-create-target-index.sh
./04-configure-target-index.sh
```

The target must be created through `PUT /_mv/views/{name}`. Do not issue a direct `PUT /cb100m_mv`: the view service creates the dedicated DerivedEngine index and persists its source binding and definition metadata.

Override names or endpoint without editing scripts:

```bash
OS_URL=http://localhost:9200 SOURCE_INDEX=cb100m TARGET_INDEX=cb100m_mv VIEW_NAME=cb100m_mv ./03-create-target-index.sh
```
