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
