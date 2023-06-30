# kafka_exporter_importer
Import and export kafka topic data


# To build
```bash
$ cargo build --release
```

# To export data
```bash
$ target/release/exporter --help
Usage: exporter [OPTIONS] --topic <TOPIC>

Options:
  -c, --command-config <COMMAND_CONFIG>    [default: client.properties]
  -t, --topic <TOPIC>                      
  -l, --log-conf <LOG_CONF>                [default: rdkafka=warn]
  -o, --out-file <OUT_FILE>                [default: export.out]
      --threads <THREADS>                  [default: 20]
      --report-interval <REPORT_INTERVAL>  [default: 3000]
  -h, --help                               Print help
```

where:

*COMMAND_CONFIG*: the client.properties file to use to connect to kafka

*TOPIC*: The topic to export

*OUT_FILE*: Output file (json)

*THREADS*: Parallel threads

*REPORT_INTERVAL*: Print progress after every *REPORT_INTERVAL* messages had been exported

You can optionally set the log level of rdkafka by using *LOG_CONF*.

# To import data
```bash
$ target/release/importer --help
Usage: importer [OPTIONS] --topic <TOPIC>

Options:
  -c, --command-config <COMMAND_CONFIG>    [default: client.properties]
  -t, --topic <TOPIC>                      
  -l, --log-conf <LOG_CONF>                [default: rdkafka=warn]
  -i, --input-file <INPUT_FILE>            [default: export.out*]
      --report-interval <REPORT_INTERVAL>  [default: 3000]
      --random-partition                   
      --keep-timestamp                     
      --threads <THREADS>                  [default: 20]
  -h, --help                               Print help
```

where:

*COMMAND_CONFIG*: the client.properties file to use to connect to kafka

*TOPIC*: The topic to export

*INPUT_FILE*: Input files (json). You can use wildcards like `/opt/my_export_files_*`

*THREADS*: Parallel threads

*REPORT_INTERVAL*: Print progress after every *REPORT_INTERVAL* messages had been exported

*--random-partition*: Reshuffle all data to different partitions (not recommended)

*--keep-timestamp*: Use exported entry's timestamp as message timestamp instead of now (not recommended).

NOTE: If you keep timestamp, due to kafka retention setting, messages might be deleted immediately.


# Sample export payload

All attributes are preserved including:

1. Headers (header names, null values or actual values). Header values are represented by base64 encoded strings (byte array)
2. Topic name
3. Partition number
4. Offset
5. Key (keys are either null if they are not present, or base64 encoded byte arrays)
6. Value (values are either null if they are not present, or base64 encoded byte arrays)
7. Timestamp (timestamp typically is not used to import by default due to data retention issues)

```json
{
  "headers": [
    {
      "Header-aqcolmv": null
    },
    {
      "Header-vjlterlwhbqowocurcix": "d4WvtTpKqzAdhNEpTcUSItfKVV9PkwhvMDO1QawABrLU0pyVfV0DvqU5bfQ0p1WFm6E"
    },
    {
      "Header-wuubvnwcnykxbsvvwrqd": "Re4Fg2J3WfqbuE7M24BViNDCbePdjvGfgqWk0vAp7mJD2T0D3pX/Fuqq7jxD7lO1V7s"
    },
    {
      "Header-gjcqyemcllkp": "es34d7jS8VmBIQkFjkoXEAODf81PEZqVUVmvZWT16evJ+w2hnqpbIN8FdLzVkR0W+3I"
    }
  ],
  "key": "b25lc3B5bHNxd3prLTA",
  "offset": 0,
  "partition": 0,
  "timestamp": 1688095128392,
  "topic": "def",
  "value": "eyJuYW1lIjogIkN1c3RvbWVyLU5hbWUtMCIsICJlbWFpbCI6ICJDdXN0b21lci1FbWFpbC0wQGNvbmZsdWVudC5pbyJ9"
}
```
