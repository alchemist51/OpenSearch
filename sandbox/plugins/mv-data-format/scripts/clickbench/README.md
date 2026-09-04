# ClickBench MV setup

Each numbered script contains exactly one literal `curl` request. Index names, URLs, and request bodies are fixed and directly inspectable—there is no runtime payload generation.

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

Static request bodies:

- `cb100m-source-index.json`
- `cb100m-mv-validate.json`
- `cb100m-mv-create.json`
- `cb100m-mv-settings.json`

The target must be created through `PUT /_mv/views/cb100m_mv`. Do not issue a direct `PUT /cb100m_mv`: the view service creates the dedicated DerivedEngine index and persists its source binding and definition metadata.
