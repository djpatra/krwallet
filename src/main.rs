use std::{env, error::Error, fs::File};

use csv::{ReaderBuilder, Trim};
use krwallet::{wallet::processor::TransactionProcessor, CsvStream};

const ACTOR_COUNT: usize = 10;

const BUFFER_SIZE:usize = 100;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input_file.csv>", args[0]);
        std::process::exit(1);
    }

    // The main function is now just responsible for I/O and orchestration.
    // It's a light interface between the CLI to the core logic.


    let mut builder = tokio::runtime::Builder::new_multi_thread();
    (1..=ACTOR_COUNT).for_each(|thread| {
        builder.worker_threads(thread);
    });

    // The code should fail if Tokio runtime could not be build
    let runtime = builder.enable_all().build().unwrap();
    
    let input_file = File::open(&args[1])?;
    let reader = ReaderBuilder::new()
                .trim(Trim::All)
                .from_reader(input_file);

    
    runtime.block_on(async move {
        let mut transaction_processor = TransactionProcessor::new(ACTOR_COUNT, BUFFER_SIZE).await;   

        transaction_processor.process(CsvStream {
                reader,
        }).await;

        // transaction_processor.output();
    });

    Ok(())
}
