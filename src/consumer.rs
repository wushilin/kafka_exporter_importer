use log::{info, warn};

pub mod cliutil;
use clap::Parser;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::message::Headers;
use rdkafka::util::get_rdkafka_version;

use crate::logutil::setup_logger;
use rdkafka::client::ClientContext;
use rdkafka::consumer::stream_consumer::StreamConsumer;

//use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;
type LoggingConsumer = StreamConsumer<CustomContext>;

mod logutil;
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

#[derive(Parser, Debug, Clone)]
pub struct CliArg {
    #[arg(short, long, default_value_t=String::from("client.properties"))]
    pub command_config: String,
    #[arg(short, long)]
    pub topic: String,
    #[arg(short, long, default_value_t=String::from("rdkafka=trace"))]
    pub log_conf: String,
}
#[tokio::main]
async fn main() {
    let args = CliArg::parse();
    let command_config = args.command_config;
    let topic = args.topic;
    let log_conf = args.log_conf;
    setup_logger(true, Some(log_conf.as_str()));
    let mut config = cliutil::load_config(&command_config).expect("Config can't be loaded");
    println!("Loaded config: {config:#?}");
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);
    consume(&mut config, &topic).await;
}


async fn consume(config: &mut ClientConfig, topic_name: &String) {
    let context = CustomContext;
    let consumer: LoggingConsumer = config
    .create_with_context(context)
    .expect("Consumer creation error");
    let topics_ve = vec!(topic_name.as_str());
    consumer
        .subscribe(&topics_ve)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                //let key = m.key();
                //let payload_bytes = m.payload();
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        match header.value {
                            Some(bs) => {
                                info!("  Header {:#?}: {:?}", header.key, std::str::from_utf8(bs).expect("Can't convert header as string"));
                            }, 
                            None => {
                                info!("  Header {:#?}: None", header.key);
                            }
                        }
                    }
                }
                consumer.store_offset(m.topic(), m.partition(), m.offset()).unwrap();
                //consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}