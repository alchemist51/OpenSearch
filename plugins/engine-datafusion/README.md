
## Prerequisites

1. Checkout branch `substrait-plan` for OpenSearch SQL Plugin - https://github.com/vinaykpud/sql/tree/substrait-plan OR  https://github.com/bharath-techie/sql/tree/substrait-plan

2. Publish OpenSearch to maven local
```
./gradlew publishToMavenLocal -Dbuild.snapshot=false
```
3. Publish SQL plugin to maven local
```
./gradlew publishToMavenLocal -Dbuild.snapshot=false
```
4. Run opensearch with following parameters
```
 ./gradlew run --preserve-data -PremotePlugins="['org.opensearch.plugin:opensearch-job-scheduler:3.3.0.0', 'org.opensearch.plugin:opensearch-sql-plugin:3.3.0.0']" -PinstalledPlugins="['engine-datafusion']" -Dbuild.snapshot=false --debug-jvm
```


## Steps to test indexing + search e2e

TODO : need to remove hardcoded index name `index-7`

1. Delete previous index if any
```
curl --location --request DELETE 'localhost:9200/index-7'
```

2. Create index with name : `index-7`
```
curl --location --request PUT 'http://localhost:9200/index-7' \
--header 'Content-Type: application/json' \
--data-raw '{
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": -1,
        "optimized.enabled": true
    },
    "mappings": {
        "dynamic": "false",
        "properties": {
           "AdvEngineID": {
             "type": "short"
          },
          "Age": {
            "type": "short"
          },
          "BrowserCountry": {
            "type": "keyword"
          },
          "BrowserLanguage": {
            "type": "keyword"
          },
          "CLID": {
            "type": "integer"
          },
          "ClientEventTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
          },
          "ClientIP": {
            "type": "integer"
          },
          "ClientTimeZone": {
            "type": "short"
          },
          "CodeVersion": {
            "type": "integer"
          },
          "ConnectTiming": {
            "type": "integer"
          },
          "CookieEnable": {
            "type": "short"
          },
          "CounterClass": {
            "type": "short"
          },
          "CounterID": {
            "type": "integer"
          },
          "DNSTiming": {
            "type": "integer"
          },
          "DontCountHits": {
            "type": "short"
          },
          "EventDate": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
          },
          "EventTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
          },
          "FUniqID": {
            "type": "long"
          },
          "FetchTiming": {
            "type": "integer"
          },
          "FlashMajor": {
            "type": "short"
          },
          "FlashMinor": {
            "type": "short"
          },
          "FlashMinor2": {
            "type": "short"
          },
          "FromTag": {
            "type": "keyword"
          },
          "GoodEvent": {
            "type": "short"
          },
          "HID": {
            "type": "integer"
          },
          "HTTPError": {
            "type": "short"
          },
          "HasGCLID": {
            "type": "short"
          },
          "HistoryLength": {
            "type": "short"
          },
          "HitColor": {
            "type": "keyword"
          },
          "IPNetworkID": {
            "type": "integer"
          },
          "Income": {
            "type": "short"
          },
          "Interests": {
            "type": "short"
          },
          "IsArtifical": {
            "type": "short"
          },
          "IsDownload": {
            "type": "short"
          },
          "IsEvent": {
            "type": "short"
          },
          "IsLink": {
            "type": "short"
          },
          "IsMobile": {
            "type": "short"
          },
          "IsNotBounce": {
            "type": "short"
          },
          "IsOldCounter": {
            "type": "short"
          },
          "IsParameter": {
            "type": "short"
          },
          "IsRefresh": {
            "type": "short"
          },
          "JavaEnable": {
            "type": "short"
          },
          "JavascriptEnable": {
            "type": "short"
          },
          "LocalEventTime": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis"
          },
          "MobilePhone": {
            "type": "short"
          },
          "MobilePhoneModel": {
            "type": "keyword"
          },
          "NetMajor": {
            "type": "short"
          },
          "NetMinor": {
            "type": "short"
          },
          "OS": {
            "type": "short"
          },
          "OpenerName": {
            "type": "integer"
          },
          "OpenstatAdID": {
            "type": "keyword"
          },
          "OpenstatCampaignID": {
            "type": "keyword"
          },
          "OpenstatServiceName": {
            "type": "keyword"
          },
          "OpenstatSourceID": {
            "type": "keyword"
          },
          "OriginalURL": {
            "type": "keyword"
          },
          "PageCharset": {
            "type": "keyword"
          },
          "ParamCurrency": {
            "type": "keyword"
          },
          "ParamCurrencyID": {
            "type": "short"
          },
          "ParamOrderID": {
            "type": "keyword"
          },
          "ParamPrice": {
            "type": "long"
          },
          "Params": {
            "type": "keyword"
          },
          "Referer": {
            "type": "keyword"
          },
          "RefererCategoryID": {
            "type": "short"
          },
          "RefererHash": {
            "type": "long"
          },
          "RefererRegionID": {
            "type": "integer"
          },
          "RegionID": {
            "type": "integer"
          },
          "RemoteIP": {
            "type": "integer"
          },
          "ResolutionDepth": {
            "type": "short"
          },
          "ResolutionHeight": {
            "type": "short"
          },
          "ResolutionWidth": {
            "type": "short"
          },
          "ResponseEndTiming": {
            "type": "integer"
          },
          "ResponseStartTiming": {
            "type": "integer"
          },
          "Robotness": {
            "type": "short"
          },
          "SearchEngineID": {
            "type": "short"
          },
          "SearchPhrase": {
            "type": "keyword"
          },
          "SendTiming": {
            "type": "integer"
          },
          "Sex": {
            "type": "short"
          },
          "SilverlightVersion1": {
            "type": "short"
          },
          "SilverlightVersion2": {
            "type": "short"
          },
          "SilverlightVersion3": {
            "type": "integer"
          },
          "SilverlightVersion4": {
            "type": "short"
          },
          "SocialSourceNetworkID": {
            "type": "short"
          },
          "SocialSourcePage": {
            "type": "keyword"
          },
          "Title": {
            "type": "keyword"
          },
          "TraficSourceID": {
            "type": "short"
          },
          "URL": {
            "type": "keyword"
          },
          "URLCategoryID": {
            "type": "short"
          },
          "URLHash": {
            "type": "long"
          },
          "URLRegionID": {
            "type": "integer"
          },
          "UTMCampaign": {
            "type": "keyword"
          },
          "UTMContent": {
            "type": "keyword"
          },
          "UTMMedium": {
            "type": "keyword"
          },
          "UTMSource": {
            "type": "keyword"
          },
          "UTMTerm": {
            "type": "keyword"
          },
          "UserAgent": {
            "type": "short"
          },
          "UserAgentMajor": {
            "type": "short"
          },
          "UserAgentMinor": {
            "type": "keyword"
          },
          "UserID": {
            "type": "long"
          },
          "WatchID": {
            "type": "long"
          },
          "WindowClientHeight": {
            "type": "short"
          },
          "WindowClientWidth": {
            "type": "short"
          },
          "WindowName": {
            "type": "integer"
          },
          "WithHash": {
            "type": "short"
          }
        }
    }
}'
```
3. Index docs
```
curl --location --request POST 'http://localhost:9200/_bulk' \
--header 'Content-Type: application/json' \
--data-raw '{"index":{"_index":"index-7"}}
{"id":"1","name":"Alice","age":30,"salary":75000,"score":95.5,"active":true,"created_date":"2024-01-15"}
{"index":{"_index":"index-7"}}
{"id":"2","name":"Bob","age":25,"salary":60000,"score":88.3,"active":true,"created_date":"2024-02-20"}
{"index":{"_index":"index-7"}}
{"id":"3","name":"Charlie","age":35,"salary":90000,"score":92.7,"active":false,"created_date":"2024-03-10"}
{"index":{"_index":"index-7"}}
{"id":"4","name":"Diana","age":28,"salary":70000,"score":89.1,"active":true,"created_date":"2024-04-05"}
{"index":{"_index":"index-7"}}
{"id":"5","name":"Bob","age":30,"salary":55000,"score":81.1,"active":true,"created_date":"2024-04-05"}
{"index":{"_index":"index-7"}}
{"id":"5","name":"Diana","age":35,"salary":65000,"score":71.1,"active":true,"created_date":"2024-02-05"}
'
```
4. Refresh the index
```
curl localhost:9200/index-7/_refresh
```
5. Query
```
curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count(), min(age) as min, max(age) as max, avg(age) as avg"
}'


curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count() as c by name | sort c"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats count(), sum(age) as c by name | sort c"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | where name = \"Bob\" | stats sum(age)"
}'


curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats sum(age) as s by name | sort s"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' --header 'Content-Type: application/json' --data-raw '{
  "query": "source=index-7 | stats sum(age) as s by name | sort name"
}'

curl --location --request POST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data-raw '{
  "query": "source=index-7 | stats count() as c by name"
}'
```

## Steps to Run Unit Tests for Search Flow

Run the following command in **OpenSearch** to execute tests
```
./gradlew :plugins:engine-datafusion:test --tests "org.opensearch.datafusion.DataFusionReaderManagerTests"
```
