use std::path::PathBuf;

use clap::Parser;


/// A struct to hold the command-line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about = "A simple CLI tool to process financial transactions.", long_about = None)]
struct Cli {
    /// The path to the input CSV file containing transactions.
    #[arg(value_name = "INPUT_FILE")]
    input_file: PathBuf,
}
