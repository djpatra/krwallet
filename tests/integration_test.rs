use std::io::Cursor;

use csv::{Reader, Writer};
use krwallet::{wallet::processor::TransactionProcessor, CsvStreamReader, CsvStreamWriter};


#[tokio::test]
async fn test_basic_transactions() {
    let csv_data = r#"type,client,tx,amount
deposit,1,1,1.0
deposit,2,2,2.0
deposit,1,3,2.0
withdrawal,1,4,1.5
withdrawal,2,5,3.0"#;

    let reader = CsvStreamReader { reader: Reader::from_reader(Cursor::new(csv_data)) };
    let mut processor = TransactionProcessor::new(2, 10).await;

    processor.process(reader).await;

    let mut output = Vec::new();
    let writer = CsvStreamWriter { writer: Writer::from_writer(&mut output) };
    processor.output(writer).await;
    
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("1,1.5000,0.0000,1.5000,false"));
    assert!(output_str.contains("2,2.0000,0.0000,2.0000,false"));
}

#[tokio::test]
async fn test_dispute_flow() {
    let csv_data = r#"type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
resolve,1,1,"#;

    let reader = CsvStreamReader { reader: Reader::from_reader(Cursor::new(csv_data)) };
    let mut processor = TransactionProcessor::new(2, 10).await;
    
    processor.process(reader).await;

    let mut output = Vec::new();
    let writer = CsvStreamWriter { writer: Writer::from_writer(&mut output) };
    processor.output(writer).await;

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("1,10.0000,0.0000,10.0000,false"));
}

#[tokio::test]
async fn test_chargeback() {
    let csv_data = r#"type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
chargeback,1,1,"#;

    let reader = CsvStreamReader { reader: Reader::from_reader(Cursor::new(csv_data)) };
    let mut processor = TransactionProcessor::new(2, 10).await;
    
    processor.process(reader).await;
    
    let mut output = Vec::new();
    let writer = CsvStreamWriter { writer: Writer::from_writer(&mut output) };
    processor.output(writer).await;

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("1,0.0000,0.0000,0.0000,true"));
}

#[tokio::test]
async fn test_insufficient_funds() {
    let csv_data = r#"type,client,tx,amount
deposit,1,1,5.0
withdrawal,1,2,10.0"#;

    let reader = CsvStreamReader { reader: Reader::from_reader(Cursor::new(csv_data)) };
    let mut processor = TransactionProcessor::new(2, 10).await;
    
    processor.process(reader).await;
    
    let mut output = Vec::new();
    let writer = CsvStreamWriter { writer: Writer::from_writer(&mut output) };
    processor.output(writer).await;

    let output_str = String::from_utf8(output).unwrap();
    // Withdrawal should be rejected, balance remains 5.0
    assert!(output_str.contains("1,5.0000,0.0000,5.0000,false"));
}

#[tokio::test]
async fn test_locked_account() {
    let csv_data = r#"type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
chargeback,1,1,
deposit,1,2,5.0"#;

    let reader = CsvStreamReader { reader: Reader::from_reader(Cursor::new(csv_data)) };
    let mut processor = TransactionProcessor::new(2, 10).await;
    
    processor.process(reader).await;
    
    let mut output = Vec::new();
    let writer = CsvStreamWriter { writer: Writer::from_writer(&mut output) };
    processor.output(writer).await;

    let output_str = String::from_utf8(output).unwrap();
    // Account should be locked, second deposit rejected
    assert!(output_str.contains("1,0.0000,0.0000,0.0000,true"));
}
