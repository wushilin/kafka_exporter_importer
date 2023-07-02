use fs::File;
use java_properties::read;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::ClientConfig;
use std::collections::HashMap;
use std::fs;
use std::io::BufReader;

pub fn load_properties(
    filename: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let f = File::open(filename)?;
    let result = read(BufReader::new(f))?;
    return Ok(result);
}

pub fn load_config(filename: &str) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let map = load_properties(filename)?;
    let mut result = ClientConfig::new();
    result.extend(map);
    result.log_level = RDKafkaLogLevel::Warning;
    return Ok(result);
}
