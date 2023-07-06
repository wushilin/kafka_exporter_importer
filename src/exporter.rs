use log::{debug, error, info};

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
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::errors::GeneralError;
extern crate lazy_static;
pub mod errors;
pub mod schemaregistry;
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
    #[arg(
        long,
        default_value_t = 3000,
        help = "reporting interval for number of records exporterd"
    )]
    pub report_interval: u64,

    #[arg(long, default_value_t=String::from(""), help="schema export file")]
    pub schemas_out: String,

    #[arg(
        long,
        default_value_t = false,
        help = "skip records with invalid schema id"
    )]
    pub skip_invalid_schema: bool,
}

#[tokio::main]
async fn main() {
    let args = CliArg::parse();
    let command_config = args.command_config;
    let topic = args.topic;
    let log_conf = args.log_conf;
    let out_file = args.out_file;
    let schema_output_file = args.schemas_out;
    let report_interval: u64 = args.report_interval;
    setup_logger(true, Some(log_conf.as_str()));
    let output_glob = format!("{out_file}_partition_*");
    let glob_output = glob::glob(&output_glob);
    let mut existing_output_files = Vec::new();
    let mut glob_count: usize = 0;
    let skip_invalid_schema = args.skip_invalid_schema;
    for i in glob_output.unwrap() {
        glob_count += 1;
        existing_output_files.push(i);
    }
    if glob_count > 0 {
        error!("Output file `{out_file}` matched {glob_count} file(s). Please delete them first.");
        for (index, next) in existing_output_files.iter().enumerate() {
            let number = index + 1;
            let next = next.as_ref().unwrap();
            let next = next.as_os_str();
            println!(" {number:5}: {next:#?}");
        }
        return;
    }

    if std::path::Path::new(&schema_output_file).exists() {
        error!("Schema output file `{schema_output_file}` already exists. Please delete it first.");
        return;
    }
    let mut config = cliutil::load_properties(&command_config).expect("Config can't be loaded");
    info!("Loaded config: {config:#?}");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    run_export(
        &mut config,
        &topic,
        &out_file,
        report_interval,
        skip_invalid_schema,
        schema_output_file,
    )
    .await;
}

async fn run_export_partition(
    config: ClientConfig,
    topic_name: &str,
    partition: i32,
    out_file: &str,
    report_interval: u64,
    sr_client: Arc<Mutex<Option<schemaregistry::SrClient>>>,
    skip_invalid_schema: bool,
    enable_schema_export: bool,
) -> Result<(usize, usize, HashMap<i32, String>, HashMap<i32, String>), Box<dyn Error>> {
    let mut sr_cache_key = HashMap::new();
    let mut sr_cache_value = HashMap::new();
    let local_sr_client = sr_client;
    let consumer: BaseConsumer = config.create().expect("Consumer creation error");
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic_name, partition, Offset::Beginning)
        .unwrap();
    consumer
        .assign(&assignment)
        .expect("Failed to assign partitions");
    info!("Successfully assigned to partition {partition}  for {topic_name}");
    let mut file = util::open_new(out_file)?;
    let mut eol_count = 0;
    let mut message_count = 0u64;
    let mut bytes_count: u64 = 0u64;
    let mut last_id = 0;
    loop {
        let messages = consumer.poll(Timeout::After(Duration::from_millis(500)));
        match messages {
            Some(result) => {
                match result {
                    Ok(real_message) => {
                        eol_count = 0;
                        message_count += 1;
                        let message_key = real_message.key();
                        let message_value = real_message.payload();

                        if enable_schema_export {
                            let schema_id_key = util::get_schema_id(message_key);
                            let schema_id_value = util::get_schema_id(message_value);
                            //
                            if let Some(schema_id) = schema_id_key {
                                if !sr_cache_key.contains_key(&schema_id) {
                                    println!("Fetching schema id {schema_id}");
                                    let mut tmp = local_sr_client.lock().await;
                                    let sr_opt_locked = tmp.as_mut();
                                    if let Some(sr_opt) = sr_opt_locked {
                                        let schema_r = sr_opt.get_schema_by_id(schema_id).await;
                                        if skip_invalid_schema && schema_r.is_err() {
                                            continue;
                                        }
                                        let schema_r = schema_r?;
                                        sr_cache_key.insert(schema_id, schema_r.clone());
                                    }
                                    drop(tmp);
                                    tokio::task::yield_now().await;
                                    // export_schema
                                }
                            }
                            if let Some(schema_id) = schema_id_value {
                                if !sr_cache_value.contains_key(&schema_id) {
                                    let mut tmp = local_sr_client.lock().await;
                                    let sr_opt_locked = tmp.as_mut();
                                    if let Some(sr_opt) = sr_opt_locked {
                                        let schema_r = sr_opt.get_schema_by_id(schema_id).await;
                                        if skip_invalid_schema && schema_r.is_err() {
                                            continue;
                                        }
                                        let schema_r = schema_r?;
                                        if schema_id != last_id {
                                            last_id = schema_id;
                                        }
                                        sr_cache_value.insert(schema_id, schema_r.clone());
                                    }
                                    drop(tmp);
                                    tokio::task::yield_now().await;
                                    // export_schema
                                }
                            }
                        }
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
    return Ok((
        message_count as usize,
        bytes_count as usize,
        sr_cache_key,
        sr_cache_value,
    ));
}

async fn run_export(
    config_map: &mut HashMap<String, String>,
    topic_name: &str,
    out_file: &str,
    report_interval: u64,
    skip_invalid_schema: bool,
    schema_output_file: String,
) {
    let config = cliutil::properties_to_config(config_map);
    let consumer: BaseConsumer = config.create().expect("Consumer creation error");
    let mut schema_registry_client = None;
    if schema_output_file.len() > 0 {
        if schema_output_file == "" {
            error!("Enable schema export requires schema output file!");
            return;
        }
        let sr_client = schemaregistry::SrClient::from_map(config_map).unwrap();
        schema_registry_client = Some(sr_client);
    }
    let enable_schema_export = schema_output_file.len() > 0;
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

    let mut jhs = Vec::new();

    let sr_client = Arc::new(Mutex::new(schema_registry_client));
    for p in 0..partition_count {
        let sr_client = Arc::clone(&sr_client);
        let new_config = config.clone();
        let new_topic_name = String::from(topic_name);
        let thread_out_file = format!("{out_file}_partition_{p}");
        let jh = tokio::spawn(async move {
            let result = run_export_partition(
                new_config,
                &new_topic_name,
                p,
                &thread_out_file,
                report_interval,
                sr_client,
                skip_invalid_schema,
                enable_schema_export,
            )
            .await;
            match result {
                Ok(what) => {
                    return Ok(what);
                }
                Err(cause) => {
                    return Err(GeneralError::from_string(format!("{cause:#?}")));
                }
            }
            //return (tid, msgs, bytes, sr_cache);
        });
        jhs.push(jh);
    }

    let mut final_msg_count: usize = 0;
    let mut final_bytes: usize = 0;

    let mut global_cache_key = HashMap::new();
    let mut global_cache_value = HashMap::new();
    let mut ready_count = 0;
    while ready_count < partition_count {
        // Check exported messages and total bytes
        //let (_, msgs, bytes, sr_cache) = rx.recv().unwrap();
        //final_msg_count += msgs;
        //final_bytes += bytes;
        //done_count += 1;
        //global_cache.extend(sr_cache);
        for (index, jh) in jhs.iter().enumerate() {
            if jh.is_finished() {
                let jh1 = jhs.remove(index);
                let result = jh1.await.unwrap();
                match result {
                    Err(cause) => {
                        println!("{cause}");
                        error!("A thread failed with error: {cause}");
                    }
                    Ok((msgs, bytes, sr_cache_key, sr_cache_value)) => {
                        final_msg_count += msgs;
                        final_bytes += bytes;
                        global_cache_key.extend(sr_cache_key);
                        global_cache_value.extend(sr_cache_value);
                    }
                }
                ready_count += 1;
                break;
            }
        }
    }
    info!("{partition_count}/{partition_count} tasks done!");
    info!("Total {final_msg_count} records, {final_bytes} bytes exported.");

    let schema_count = global_cache_key.len() + global_cache_value.len();
    if enable_schema_export {
        if schema_count > 0 {
            info!("Exporting {schema_count} relevant schema(s).");
            let mut out_schema_file = util::open_new(&schema_output_file);
            if let Ok(out_schema_file) = out_schema_file.as_mut() {
                let json = json!({ "source_schemas_keys": global_cache_key, "source_schemas_values": global_cache_value });
                let str = json.to_string();
                out_schema_file
                    .write(str.as_bytes())
                    .expect(format!("Can't write file {schema_output_file}").as_str());
                info!("Exprted {schema_count} schema(s) to `{schema_output_file}`");
                info!("You can use the schema file for importing later.");
            } else {
                error!("Failed to open {schema_output_file} for writing");
            }
        } else {
            info!("No schema to export");
        }
    } else {
        info!("Schemas are not exported. Schema ID copied as is");
    }
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
