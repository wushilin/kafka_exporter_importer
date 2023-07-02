use log::{error, info, warn};

pub mod cliutil;
pub mod iterators;
pub mod kafkautil;

use crate::util::base64_decode;
use clap::Parser;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::get_rdkafka_version;
use serde_json::Value;
use std::io::BufReader;
use std::sync::mpsc::channel;
use std::time::Duration;

pub mod util;

pub mod logutil;
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
    logutil::setup_logger(true, Some(log_conf.as_str()));

    let mut config = cliutil::load_config(&command_config).expect("Config can't be loaded");
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

    if !honor_partition && honor_timestamp {
        warn!("NOTE: not honoring partition will cause data to be produced to random partition.
             In order to preserve the timestamp based order, this program will logically merge all files 
             based on the timestamp field, and produce in the timestamp order in a single thread.
             
             This is slower than honoring partition (which can be parallel executed).
             
             If you use --replace-timestamp, parallel execution is possible as order is not important.
             ");
        let mut iters = Vec::<Box<dyn Iterator<Item = Result<Value, serde_json::Error>>>>::new();
        for next in file_names {
            iters.push(get_iterator(&next));
        }

        let ts_sensitive = |x: &[&Result<Value, serde_json::Error>]| -> i32 {
            let mut result = -1;
            let mut selected: Option<&Value> = None;
            for (i, ele) in x.iter().enumerate() {
                if let Err(_) = *ele {
                    continue;
                }
                let ele_real = ele.as_ref().unwrap();
                if result < 0 {
                    result = i as i32;
                    selected = Some(ele_real);
                } else {
                    if let Some(min) = selected {
                        let mints = min.get("timestamp").unwrap();
                        let this_ts = ele_real.get("timestamp").unwrap();

                        if let Value::Number(mints_number) = mints {
                            if let Value::Number(thists_number) = this_ts {
                                if thists_number.as_i64() < mints_number.as_i64() {
                                    result = i as i32;
                                    selected = Some(ele_real);
                                }
                            }
                        }
                    }
                }
            }
            result
        };

        if honor_timestamp {
            let multi_iter = iterators::MultiplexedIterator::from_iters(iters, ts_sensitive);
            let local_gc = Arc::clone(&global_import_counter);
            run_import(
                &mut config,
                String::from(""),
                Box::new(multi_iter),
                &topic,
                honor_partition,
                honor_timestamp,
                report_interval,
                local_gc,
            );
        } else {
            // this should not happen
            let multi_iter = iterators::SequentialIterator::from_iters(iters);
            let local_gc = Arc::clone(&global_import_counter);
            run_import(
                &mut config,
                String::from(""),
                Box::new(multi_iter),
                &topic,
                honor_partition,
                honor_timestamp,
                report_interval,
                local_gc,
            );
        }
    } else {
        if file_count > partition_count as usize {
            error!("Apparently impossible to honor partitions: Partition count (according to input file count) {file_count} > actual topic partition count {partition_count}");
            return;
        }
        if file_count < partition_count as usize {
            warn!("Partition count(according to input file count) {file_count} < actual topic partition count {partition_count}, some partition won't receive data");
        }
        let mut actual_threads = threads;
        if actual_threads > file_count as usize {
            info!("Using only {file_count} threads as {threads} is larger than required.");
            actual_threads = partition_count as usize;
        }
        let pool = threadpool::Builder::new()
            .thread_name(format!("worker").into())
            .num_threads(actual_threads)
            .build();
        let (tx, rx) = channel();
        for i in 0..file_count {
            let mut new_config = config.clone();
            let new_topic_name = String::from(topic.as_str());
            let tx = tx.clone();
            let tid = i;
            let path = file_names.pop().unwrap();
            let local_gc = Arc::clone(&global_import_counter);
            pool.execute(move || {
                run_partition(
                    &mut new_config,
                    path,
                    new_topic_name,
                    honor_partition,
                    honor_timestamp,
                    report_interval,
                    local_gc,
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
        pool.join();
    }
    let final_result = global_import_counter.load(Ordering::SeqCst);
    info!("Total imported: {final_result}");
}

fn run_partition(
    config: &mut ClientConfig,
    file: std::path::PathBuf,
    target_topic: String,
    honor_partition: bool,
    honor_timestamp: bool,
    report_interval: u64,
    global_counter: Arc<AtomicUsize>,
) {
    let iterator = get_iterator(&file);
    run_import(
        config,
        file.clone().into_os_string().into_string().unwrap(),
        iterator,
        &target_topic,
        honor_partition,
        honor_timestamp,
        report_interval,
        global_counter,
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
) -> Box<dyn Iterator<Item = Result<serde_json::Value, serde_json::Error>>> {
    let file_handle = std::fs::File::open(file)
        .expect(format!("Failed to open file for reading: {file:#?}").as_str());
    let deserializer = serde_json::Deserializer::from_reader(BufReader::new(file_handle));
    let iter = deserializer.into_iter::<Value>();
    return Box::new(iter);
}

fn run_import(
    config: &mut ClientConfig,
    file_name: String,
    items: Box<dyn Iterator<Item = Result<Value, serde_json::Error>>>,
    target_topic_name: &str,
    honor_partition: bool,
    honor_timestamp: bool,
    report_interval: u64,
    global_counter: Arc<AtomicUsize>,
) {
    let producer_context = DefaultProducerContext;
    let producer: &ThreadedProducer<DefaultProducerContext> = &config
        .create_with_context(producer_context)
        .expect("Producer creation error");
    let mut imported_records: usize = 0;
    for item in items {
        match item {
            Ok(entry) => {
                let headers_raw = entry.get("headers").unwrap().as_array().unwrap();
                let headers = parse_headers(headers_raw);
                let key = entry.get("key");
                let value = entry.get("value");
                let partition = entry.get("partition").unwrap().as_i64().unwrap();
                let timestamp = entry.get("timestamp").unwrap().as_i64().unwrap();
                let key_bb = value_to_bytes(key);
                let value_bb = value_to_bytes(value);
                loop {
                    let mut new_record = BaseRecord::to(target_topic_name);
                    if let Some(key_real) = key_bb.as_ref() {
                        new_record = new_record.key(key_real);
                    }
                    if let Some(value_real) = value_bb.as_ref() {
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
    if file_name.len() == 0 {
        info!("Done. {imported_records} entries imported");
    } else {
        info!("File `{file_name}` done. {imported_records} entries imported")
    }
}
