use std::fs;
use std::collections::HashMap;
use fs::File;
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use std::io::BufReader;
use java_properties::read;

pub fn load_properties(filename:&str) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let result = read(BufReader::new(f))?;
    return Ok(result);
}

pub fn load_config(filename:&str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let map = load_properties(filename)?;
    let mut result = ClientConfig::new();
    result.extend(map);
    result.log_level = RDKafkaLogLevel::Warning;
    return Ok(result);
}