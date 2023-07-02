use log::{debug, info};

pub mod cliutil;
pub mod logutil;
use clap::Parser;
use logutil::setup_logger;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::get_rdkafka_version;
use rdkafka::util::Timeout;
use rdkafka::Message;
use serde_json::{json, Map, Value};
use std::io::Write;
use std::sync::mpsc::channel;
use std::time::Duration;

pub mod util;

#[derive(Parser, Debug, Clone)]
pub struct CliArg {
    #[arg(short, long, default_value_t=String::from("client.properties"), help="your kafka client.properties")]
    pub command_config: String,
    #[arg(short, long, help = "topic name to export")]
    pub topic: String,
    #[arg(short, long, default_value_t=String::from("rdkafka=warn"), help="log level (DEBUG|INFO|WARN|ERROR)")]
    pub log_conf: String,
    #[arg(short, long, default_value_t=String::from("export.out"), help="file prefix. `_partition_x` suffix might be added")]
    pub out_file: String,
    #[arg(long, default_value_t = 20, help = "number of threads to use")]
    pub threads: usize,
    #[arg(
        long,
        default_value_t = 3000,
        help = "reporting interval for number of records exporterd"
    )]
    pub report_interval: u64,
}

#[tokio::main]
async fn main() {
    let args = CliArg::parse();
    let command_config = args.command_config;
    let topic = args.topic;
    let log_conf = args.log_conf;
    let out_file = args.out_file;
    let threads = args.threads;
    let report_interval: u64 = args.report_interval;
    setup_logger(true, Some(log_conf.as_str()));
    let mut config = cliutil::load_config(&command_config).expect("Config can't be loaded");
    info!("Loaded config: {config:#?}");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    run_export(&mut config, &topic, &out_file, threads, report_interval);
}

fn run_export_partition(
    config: ClientConfig,
    topic_name: &str,
    partition: i32,
    out_file: &str,
    report_interval: u64,
) -> (usize, usize) {
    let consumer: BaseConsumer = config.create().expect("Consumer creation error");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic_name, partition, Offset::Beginning)
        .unwrap();
    consumer
        .assign(&assignment)
        .expect("Failed to assign partitions");
    debug!("Successfully assigned to partition {partition}  for {topic_name}");

    let file = std::fs::File::create(out_file);
    if let Err(cause) = file {
        panic!("Failed to open file for writing {cause:#?}");
    }
    let mut file = file.unwrap();
    let mut eol_count = 0;
    let mut message_count = 0u64;
    let mut bytes_count: u64 = 0u64;
    loop {
        let messages = consumer.poll(Timeout::After(Duration::from_millis(500)));
        match messages {
            Some(result) => {
                match result {
                    Ok(real_message) => {
                        eol_count = 0;
                        message_count += 1;
                        //info!("Received message from topic {topic} partition {partition} offset {offset}");
                        let json = encode_json(&real_message);
                        let json_bytes = json.as_bytes();
                        file.write_all(json_bytes).expect("File write failure");
                        file.write_all("\r\n".as_bytes())
                            .expect("File write failure (newline)");
                        bytes_count += json_bytes.len() as u64 + 2;
                        if message_count % report_interval == 0 {
                            info!(
                                "File `{out_file}` {message_count} messages, {bytes_count} bytes."
                            );
                        }
                    }
                    Err(cause) => {
                        panic!("Kafka Error: {cause:#?}");
                    }
                }
            }
            None => {
                eol_count += 1;
                if eol_count > 3 {
                    break;
                }
            }
        }
    }
    if message_count % report_interval != 0 {
        info!("File `{out_file}` {message_count} messages, {bytes_count} bytes... Done");
    }
    file.flush().expect("Can't flush file");
    debug!("Partition {partition} done");
    return (message_count as usize, bytes_count as usize);
}
fn run_export(
    config: &mut ClientConfig,
    topic_name: &str,
    out_file: &str,
    threads: usize,
    report_interval: u64,
) {
    let consumer: BaseConsumer = config.create().expect("Consumer creation error");

    let topic_meta = consumer
        .fetch_metadata(Some(topic_name), Timeout::After(Duration::from_secs(5)))
        .expect("Failed to retrieve topic metadata after 20 seconds");

    if topic_meta.topics().len() == 0 {
        info!("Topic {topic_name} not found");
    }

    let mut partition_count = -1;
    if let Some(topic_data) = topic_meta.topics().first() {
        if topic_data.partitions().len() > 0 {
            partition_count = topic_data.partitions().last().unwrap().id() + 1;
            info!("Partition count is {partition_count}")
        }
    }

    if partition_count < 0 {
        info!("No partition info for topic `{topic_name}` available. Check topic exists.");
        return;
    }

    let mut actual_threads = threads;

    if actual_threads > partition_count as usize {
        info!("Using only {partition_count} threads as {threads} is larger than required.");
        actual_threads = partition_count as usize;
    }

    let pool = threadpool::Builder::new()
        .thread_name(format!("worker").into())
        .num_threads(actual_threads)
        .build();

    let (tx, rx) = channel();
    for p in 0..partition_count {
        let new_config = config.clone();
        let new_topic_name = String::from(topic_name);
        let thread_out_file = format!("{out_file}_partition_{p}");
        let tx = tx.clone();
        let tid = p;
        pool.execute(move || {
            let (msgs, bytes) = run_export_partition(
                new_config,
                &new_topic_name,
                p,
                &thread_out_file,
                report_interval,
            );
            tx.send((tid, msgs, bytes)).unwrap();
        });
    }

    let mut done_count = 0;
    let mut final_msg_count: usize = 0;
    let mut final_bytes: usize = 0;

    for _ in 0..partition_count {
        // Check exported messages and total bytes
        let (_, msgs, bytes) = rx.recv().unwrap();
        final_msg_count += msgs;
        final_bytes += bytes;
        done_count += 1;
    }
    info!("{done_count}/{done_count} tasks done!");
    pool.join();
    info!("Total {final_msg_count} records, {final_bytes} bytes exported.");
}

fn value_for_bytes(input: Option<&[u8]>) -> Value {
    match input {
        Some(data) => {
            let str = util::base64_encode(data);
            return Value::String(str);
        }
        None => Value::Null,
    }
}

fn encode_json(message: &BorrowedMessage) -> String {
    let headers_o = message.headers();
    let mut header_vec: Vec<Value> = vec![];
    if let Some(headers) = headers_o {
        let total = headers.count();
        for index in 0..total {
            let next_header = headers.get(index);
            let key = String::from(next_header.key);
            let value = next_header.value;
            let mut next_hm: Map<String, Value> = Map::new();
            next_hm.insert(key, value_for_bytes(value));
            header_vec.push(Value::Object(next_hm));
        }
    }
    let key = message.key();
    let value = message.payload();
    let offset = message.offset();
    let partition = message.partition();
    let timestamp = message.timestamp();
    let topic = message.topic();
    let j = json!({
        "topic": topic,
        "partition": partition,
        "key": value_for_bytes(key),
        "value": value_for_bytes(value),
        "offset": offset,
        "timestamp": timestamp.to_millis().unwrap_or(-1),
        "headers":header_vec,
    });
    return j.to_string();
}
