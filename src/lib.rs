use std::str::FromStr;

use csv_async::{AsyncDeserializer, AsyncSerializer};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{mpsc::error::TrySendError, oneshot::error::RecvError};

pub mod channel_actor;
pub mod wallet;

#[derive(Error, Debug)]
pub enum ProcessorError {
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),

    #[error("Actor send error: {0}")]
    ActorTxSendError(String),

    #[error("Actor recv error: {0}")]
    ActorRecvError(String),

    #[error("Invalid amount: {message}")]
    InvalidAmount { message: String },

    #[error("Invalid transaction: {message}")]
    InvalidTransaction { message: String },

    #[error("Account locked: client {client}")]
    AccountLocked { client: u16 },

    #[error("Insufficient funds: available {available}, required {required}")]
    InsufficientFunds {
        available: rust_decimal::Decimal,
        required: rust_decimal::Decimal,
    },

    #[error("Transaction not found: {tx_id}")]
    TransactionNotFound { tx_id: u32 },

    #[error("Duplicate transaction: {tx_id}")]
    DuplicateTransaction { tx_id: u32 },

    #[error("Invalid transaction state for dispute")]
    InvalidDisputeState,

    #[error("Fatal Actor error; Exit")]
    FatalError,

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub fn map_channel_send_err<M>(err: TrySendError<M>) -> ProcessorError {
    let e = format!("{}", err);
    ProcessorError::ActorTxSendError(e)
}

pub fn map_channel_recv_err(err: RecvError) -> ProcessorError {
    let e = format!("{}", err);
    ProcessorError::ActorRecvError(e)
}

pub type ProcessorResult<T> = std::result::Result<T, ProcessorError>;

unsafe impl Send for ProcessorError {}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: TransactionType,
    pub client: u16,
    #[serde(rename = "tx")]
    pub id: u32,
    #[serde(deserialize_with = "deserialize_opt_amount")]
    pub amount: Option<Decimal>,
    #[serde(default = "default_disputed")]
    pub disputed: bool,
}

fn deserialize_opt_amount<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) if !s.trim().is_empty() => Decimal::from_str(&s).map(Some).map_err(serde::de::Error::custom),
        _ => Ok(None),
    }
}

fn default_disputed() -> bool {
    false
}

/// A streaming CSV reader
pub struct CsvStreamReader<'a> {
    pub reader: AsyncDeserializer<&'a mut tokio::fs::File>,
}

/// A streaming CSV writer
pub struct CsvStreamWriter {
    pub writer: AsyncSerializer<tokio::io::Stdout>,
}
