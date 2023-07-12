use errors::GeneralError;
use log::{error, info, warn};

pub mod cliutil;
pub mod iterators;
pub mod kafkautil;

use crate::util::base64_decode;
use clap::Parser;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer, Producer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::get_rdkafka_version;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::io::BufReader;
use std::sync::mpsc::channel;
use std::time::Duration;

pub mod schemaregistry;

pub mod errors;
pub mod logutil;
pub mod util;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Parser, Debug, Clone)]
pub struct CliArg {
    #[arg(short, long, default_value_t=String::from("client.properties"), help="your kafka client.properties")]
    pub command_config: String,
    #[arg(short, long, help = "kakfa topic to import data into")]
    pub topic: String,
    #[arg(short, long, default_value_t=String::from("rdkafka=warn"), help="log level. (DEBUG|INFO|WARN|ERROR)")]
    pub log_conf: String,
    #[arg(short, long, default_value_t=String::from("export.out*"), help="file pattern to import (e.g. `/tmp/data*`)")]
    pub input_file: String,
    #[arg(
        long,
        default_value_t = 3000,
        help = "report interval in message counts"
    )]
    pub report_interval: u64,
    #[arg(
        long,
        default_value_t = false,
        help = "use when old and new topic partition count is different"
    )]
    pub random_partition: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "keep original timestamp(not recommended)"
    )]
    pub keep_timestamp: bool,
    #[arg(
        long,
        default_value_t = 20,
        help = "use multi threading. max is the partition/file count"
    )]
    pub threads: usize,

    #[arg(long, default_value_t=String::from(""), help="also import schema and do schema conversion")]
    pub schemas_in: String,
}

fn load_schemas(
    file_name: &str,
) -> Result<(HashMap<i32, String>, HashMap<i32, String>), Box<dyn Error>> {
    let mut result_key: HashMap<i32, String> = HashMap::new();
    let mut result_value: HashMap<i32, String> = HashMap::new();
    let file = std::fs::File::open(file_name)?;
    let json_value: HashMap<String, HashMap<String, String>> = serde_json::from_reader(file)?;
    if json_value.contains_key("source_schemas_keys") {
        let inner = json_value.get("source_schemas_keys").unwrap();
        for (key, value) in inner {
            result_key.insert(key.clone().parse().unwrap(), value.clone());
        }
    } else {
        return Err(GeneralError::wrap_box(
            format!("`source_schemas_keys` not found in file `{file_name}").as_str(),
        ));
    }

    if json_value.contains_key("source_schemas_values") {
        let inner = json_value.get("source_schemas_values").unwrap();
        for (key, value) in inner {
            result_value.insert(key.clone().parse().unwrap(), value.clone());
        }
    } else {
        return Err(GeneralError::wrap_box(
            format!("`source_schemas_values` not found in file `{file_name}").as_str(),
        ));
    }
    return Ok((result_key, result_value));
}
#[tokio::main]
async fn main() {
    let global_import_counter = Arc::new(AtomicUsize::new(0));
    let args = CliArg::parse();
    let command_config = args.command_config;
    let topic = args.topic;
    let log_conf = args.log_conf;
    let input_file = args.input_file;
    let report_interval: u64 = args.report_interval;
    let honor_timestamp = args.keep_timestamp;
    let honor_partition = !args.random_partition;
    let threads = args.threads;
    let schemas_in = args.schemas_in;

    logutil::setup_logger(true, Some(log_conf.as_str()));
    if !honor_partition && honor_timestamp {
        error!("We can't do `--random-partition` and `--keep-timestamp` together. This will essentially produce message in messed up time order");
        return;
    }
    let mut schemas_key = HashMap::new();
    let mut schemas_value = HashMap::new();

    if schemas_in.len() > 0 {
        let (loaded_schemas_key, loaded_schemas_value) = load_schemas(&schemas_in).unwrap();
        schemas_key.extend(loaded_schemas_key);
        schemas_value.extend(loaded_schemas_value);
    }

    let config_map = cliutil::load_properties(&command_config).expect("Config can't be loaded");
    let mut config = cliutil::properties_to_config(&config_map);
    info!("Loaded config: {config:#?}");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    //import(&mut config, &topic, &out_file, threads, report_interval);
    //let mut iters = Vec::<Box<dyn Iterator<Item=Result<Value, serde_json::Error>>>>::new();
    let mut file_names = Vec::<std::path::PathBuf>::new();
    for entry in glob::glob(input_file.as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => {
                file_names.push(path);
            }
            Err(e) => println!("Unable to match input file: {:?}", e),
        }
    }

    let mut sr_map = HashMap::<i32, i32>::new();
    let mut sr_client =
        schemaregistry::SrClient::from_map(&config_map).expect("Failed to create SR client");

    let key_subject = format!("{topic}-key");
    let value_subject = format!("{topic}-value");
    for (id, schema_str) in schemas_key.iter() {
        let subject = key_subject.clone();
        let new_id = sr_client
            .register_schema(&subject, schema_str)
            .await
            .unwrap();
        sr_map.insert(*id, new_id);
        info!("Register schema subject `{subject}`(id=`{id}`) => id=`{new_id}`")
    }
    for (id, schema_str) in schemas_value.iter() {
        let subject = value_subject.clone();
        let new_id = sr_client
            .register_schema(&subject, schema_str)
            .await
            .unwrap();
        sr_map.insert(*id, new_id);
        info!("Register schema subject `{subject}`(id=`{id}`) => id=`{new_id}`")
    }
    // check topic exists
    let partition_count_o = kafkautil::get_topic_partition_count(&mut config, &topic);
    match partition_count_o {
        None => {
            error!("Topic `{topic}` does not exists. Please create it first!");
            return;
        }
        _ => {}
    }
    let partition_count = partition_count_o.unwrap();
    let file_count = file_names.len();

    info!("Verified that topic {topic} exists, partition count is {partition_count}");

    if file_count > partition_count as usize {
        error!("Apparently impossible to honor partitions: Partition count (according to input file count) {file_count} > actual topic partition count {partition_count}");
        return;
    }
    if file_count < partition_count as usize {
        warn!("Partition count(according to input file count) {file_count} < actual topic partition count {partition_count}, some partition won't receive data");
    }
    let mut actual_threads = partition_count as usize;
    if threads <= actual_threads {
        actual_threads = threads;
    } else {
        info!("Using {actual_threads} as only because only {actual_threads} partitions");
    }

    let pool = threadpool::Builder::new()
        .num_threads(actual_threads)
        .thread_name("worker".to_string())
        .build();
    let (tx, rx) = channel();
    for i in 0..file_count {
        let mut new_config = config_map.clone();
        let new_topic_name = String::from(topic.as_str());
        let tx = tx.clone();
        let tid = i;
        let path = file_names.pop().unwrap();
        let local_gc = Arc::clone(&global_import_counter);
        let schemas = sr_map.clone();
        pool.execute(move || {
            run_partition(
                &mut new_config,
                path,
                new_topic_name,
                honor_partition,
                honor_timestamp,
                report_interval,
                local_gc,
                schemas,
            );
            tx.send(tid).unwrap();
        });
    }
    let mut done_count = 0;
    for _ in 0..file_count {
        let _ = rx.recv().unwrap();
        done_count += 1;
    }
    info!("{done_count}/{done_count} tasks Done!");
    let final_result = global_import_counter.load(Ordering::SeqCst);
    info!("Total imported: {final_result}");

    info!("Registered schema mappings:");
    for (old, new) in sr_map {
        info!("   * OldId = {old} ==========> NewId = {new}");
    }
}

fn run_partition(
    config_map: &mut HashMap<String, String>,
    file: std::path::PathBuf,
    target_topic: String,
    honor_partition: bool,
    honor_timestamp: bool,
    report_interval: u64,
    global_counter: Arc<AtomicUsize>,
    schemas: HashMap<i32, i32>,
) {
    run_import(
        config_map,
        file.clone().into_os_string().into_string().unwrap(),
        file,
        &target_topic,
        honor_partition,
        honor_timestamp,
        report_interval,
        global_counter,
        schemas,
    );
}

fn value_to_bytes(val: Option<&Value>) -> Option<Vec<u8>> {
    match val {
        Some(value) => match value {
            Value::String(str) => {
                let bb = base64_decode(str);
                let result = bb.unwrap();
                return Some(result);
            }
            _ => None,
        },
        None => None,
    }
}

fn parse_headers(raw: &Vec<Value>) -> OwnedHeaders {
    let mut headers = OwnedHeaders::new_with_capacity(raw.len());
    for next in raw {
        match next {
            Value::Object(header) => {
                if header.len() > 0 {
                    for next_pair in header {
                        let (key, value) = next_pair;
                        match value {
                            Value::String(b64) => {
                                let bb = base64_decode(&b64).unwrap();
                                headers = headers.insert(Header {
                                    key: key.as_str(),
                                    value: Some(&bb),
                                });
                            }
                            _ => {}
                        };
                    }
                }
            }
            _ => {}
        }
    }
    return headers;
}

fn get_iterator(
    file: &std::path::PathBuf,
) -> Box<dyn Iterator<Item = Result<serde_json::Value, Box<dyn Error>>>> {
    let file_handle = std::fs::File::open(file)
        .expect(format!("Failed to open file for reading: {file:#?}").as_str());
    let deserializer = serde_json::Deserializer::from_reader(BufReader::new(file_handle));
    let iter = deserializer.into_iter::<Value>();
    let iter1 = iter.map(|what| match what {
        Ok(val) => {
            return Ok(val);
        }
        Err(cause) => {
            let boxed: Box<dyn Error> = Box::new(cause);
            return Err(boxed);
        }
    });
    return Box::new(iter1);
}

fn run_import(
    config_map: &mut HashMap<String, String>,
    file_name: String,
    path_buf: std::path::PathBuf,
    target_topic_name: &str,
    honor_partition: bool,
    honor_timestamp: bool,
    report_interval: u64,
    global_counter: Arc<AtomicUsize>,
    schemas: HashMap<i32, i32>,
) {
    let items = get_iterator(&path_buf);
    let producer_context = DefaultProducerContext;
    let config = cliutil::properties_to_config(config_map);
    let producer: &ThreadedProducer<DefaultProducerContext> = &config
        .create_with_context(producer_context)
        .expect("Producer creation error");
    let mut imported_records: usize = 0;
    let enable_schema_swap = schemas.len() > 0;
    'outer: for item in items {
        match item {
            Ok(entry) => {
                let headers_raw = entry.get("headers").unwrap().as_array().unwrap();
                let headers = parse_headers(headers_raw);
                let key = entry.get("key");
                let value = entry.get("value");
                let partition = entry.get("partition").unwrap().as_i64().unwrap();
                let timestamp = entry.get("timestamp").unwrap().as_i64().unwrap();
                let mut key_bb = value_to_bytes(key);
                let mut value_bb = value_to_bytes(value);
                loop {
                    let mut new_record = BaseRecord::to(target_topic_name);
                    // Do schema translation
                    if let Some(key_real) = key_bb.as_mut() {
                        if enable_schema_swap {
                            let schema_id = util::get_schema_id(Some(key_real));
                            if let Some(actual_schema_id) = schema_id {
                                if !schemas.contains_key(&actual_schema_id) {
                                    error!(
                                        "Schema ID {actual_schema_id} not found in schema file."
                                    );
                                    break 'outer;
                                }
                                let new_schema_id = schemas.get(&actual_schema_id).unwrap();
                                util::replace_schema_id(key_real, *new_schema_id);
                            }
                        }
                        new_record = new_record.key(key_real);
                    }
                    if let Some(value_real) = value_bb.as_mut() {
                        if enable_schema_swap {
                            let schema_id = util::get_schema_id(Some(value_real));
                            if let Some(actual_schema_id) = schema_id {
                                if !schemas.contains_key(&actual_schema_id) {
                                    error!(
                                        "Schema ID {actual_schema_id} not found in schema file."
                                    );
                                    break 'outer;
                                }
                                let new_schema_id = schemas.get(&actual_schema_id).unwrap();
                                util::replace_schema_id(value_real, *new_schema_id);
                            }
                        }
                        new_record = new_record.payload(value_real);
                    }
                    
                    if honor_timestamp {
                        if timestamp > 0 {
                            new_record = new_record.timestamp(timestamp);
                        }
                    }
                    if honor_partition {
                        new_record = new_record.partition(partition as i32);
                    } else {
                        new_record = new_record.partition(-1);
                    }
                    new_record = new_record.headers(headers.clone());
                    let produce_result = producer.send(new_record);
                    if let Err(cause) = produce_result {
                        if cause.0.rdkafka_error_code().unwrap() == RDKafkaErrorCode::QueueFull {
                            info!("Sleeping 100ms due to queue full");
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    } else {
                        imported_records += 1;
                        global_counter.fetch_add(1, Ordering::SeqCst);
                        if imported_records % (report_interval as usize) == 0 {
                            if file_name.len() == 0 {
                                info!("{imported_records} entries imported");
                            } else {
                                info!("File `{file_name}` {imported_records} entries imported")
                            }
                        }
                        break;
                    }
                }
            }
            Err(cause) => {
                warn!("Failed to decode JSON: {cause:#?}")
            }
        }
    }
    
    let start_flush = std::time::Instant::now();
    info!("Flusing producer for `{file_name}`");
    loop {
        if start_flush.elapsed() > std::time::Duration::from_secs(10) {
            error!("Failed to flush after 10 seconds!");
            break;
        }
        let flush_result = producer.flush(Duration::from_millis(5000));
        match flush_result {
            Err(_) => {
                info!("Waiting for messages to flush...(`{file_name}`)");
            },
            Ok(_) => {
                break;
            }
        }
    }
    if file_name.len() == 0 {
        info!("Done. {imported_records} entries imported");
    } else {
        info!("File `{file_name}` done. {imported_records} entries imported")
    }
}
