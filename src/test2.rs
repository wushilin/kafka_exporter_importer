use std::fs::File;
use std::io::{BufRead, BufReader};

async fn process_file(file: &str) {
    let iterator = read_file(file).await;

    for _ in iterator {

    }
}

async fn read_file(file: &str) -> Box<dyn Iterator<Item = Result<String, Box<dyn std::error::Error>>>> {
    let file = File::open(file).expect("Failed to open file");
    let reader = BufReader::new(file);

    let result = reader.lines().map(|line| {
        match line {
            Ok(_) => {
                Ok(String::from(""))
            }
            Err(cause) => {
                let boxed: Box<dyn std::error::Error> = Box::new(cause);
                Err(boxed)
            }
        }
    });
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let result = result.into_iter();
    return Box::new(result);
}

#[tokio::main]
async fn main() {
    let file_path = "path/to/your/file.txt";
    tokio::spawn(async move {
        process_file(file_path).await;
    }).await.unwrap();
}