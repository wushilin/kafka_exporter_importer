pub mod iterators;
pub mod schemaregistry;
pub mod util;
pub mod cliutil;
pub mod errors;
pub mod logutil;
#[tokio::main]
async fn main() {
    logutil::setup_logger(true, Some("DEBUG"));
    //let b = add!(1;2);
    //println!("{b}");
    let map = cliutil::load_properties("client.properties").unwrap();
    let mut client = schemaregistry::SrClient::from_map(map).unwrap();
    for _ in 0..1 {
        let body = client.get_schema_by_id(100266).await.unwrap();
        println!("{body}");
        let registered_id = client.register_schema(
            "shwu-test", &body).await;
        println!("Re-registered as {registered_id:#?}");
    }
}
