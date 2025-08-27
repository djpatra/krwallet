use std::{env, error::Error, fs::File};

use csv::{ReaderBuilder, Trim, WriterBuilder};
use krwallet::{wallet::processor::TransactionProcessor, CsvStreamReader, CsvStreamWriter};

// Someday we will read these const variables from config
const ACTOR_COUNT: usize = 4;

const BUFFER_SIZE:usize = 20;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input_file.csv>", args[0]);
        std::process::exit(1);
    }

    // The main function is only responsible for I/O and orchestration.
    // It's a light interface between the CLI to the core logic.

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    (1..=ACTOR_COUNT).for_each(|thread| {
        builder.worker_threads(thread);
    });

    // The code should fail if the runtime could not be build
    let runtime = builder.enable_all().build()?;
    
    let input_file = File::open(&args[1])?;
    let reader = ReaderBuilder::new()
                .trim(Trim::All)
                .from_reader(input_file);

    let writer = WriterBuilder::new()
                    .has_headers(true)
                    .from_writer(std::io::stdout());

    
    runtime.block_on(async move {
        let mut transaction_processor = TransactionProcessor::new(ACTOR_COUNT, BUFFER_SIZE).await;   

        // Ignoring the errors from TransactionProcessor for now
        let _ = transaction_processor.process(CsvStreamReader { reader }).await;
        
        let _ = transaction_processor.output(CsvStreamWriter { writer }).await;
    });

    Ok(())
}
