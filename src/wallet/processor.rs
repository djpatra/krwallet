use futures::StreamExt;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::{
    CsvStreamReader, CsvStreamWriter, ProcessorError, ProcessorResult, Transaction, TransactionType,
    channel_actor::{self, ActorRef},
};

use super::wallet_actor::{WalletActor, WalletActorMessages, WalletState};

pub struct TransactionProcessor {
    actor_count: usize,
    wallet_actors: Vec<ActorRef<WalletActorMessages>>,
}

#[derive(Serialize)]
struct WalletCsvView {
    client_id: u16,
    available: String,
    held: String,
    total: String,
    locked: bool,
}

impl From<WalletState> for WalletCsvView {
    fn from(state: WalletState) -> Self {
        Self {
            client_id: state.client_id,
            available: format!("{:.4}", state.wallet.available.round_dp(4)),
            held: format!("{:.4}", state.wallet.held.round_dp(4)),
            total: format!("{:.4}", state.wallet.total.round_dp(4)),
            locked: state.wallet.locked,
        }
    }
}

impl TransactionProcessor {
    /// Creates actors with bounded channels
    pub async fn new(actor_count: usize, channel_buffer_size: usize) -> Self {
        let mut wallet_actors = Vec::with_capacity(actor_count);
        for _ in 0..actor_count {
            let actor = WalletActor::create();
            let actor_ref = channel_actor::start(actor, channel_buffer_size).await;
            wallet_actors.push(actor_ref);
        }

        Self {
            actor_count,
            wallet_actors,
        }
    }

    pub async fn process(&mut self, mut stream: CsvStreamReader<'_>) -> ProcessorResult<()> {
        let mut records = stream.reader.deserialize::<Transaction>();
        while let Some(result) = records.next().await {
            let tx = match result {
                Ok(transaction) => transaction,
                Err(e) => {
                    eprintln!("Error deserializing record: {}", e);
                    continue;
                }
            };

            // Validate amount for Deposits and Withdrawl. This validation also ensures
            // that we can safely unwrap amount out of the Option
            //
            // ** Do not remove this. Removing this may make the WalletActor panic when it
            // unwraps the amount out of Option.
            if matches!(tx.tx_type, TransactionType::Deposit | TransactionType::Withdrawal) {
                let amount = tx.amount.ok_or(ProcessorError::InvalidAmount {
                    message: format!("invalid amount for tx_id={}", tx.id),
                })?;

                if amount < Decimal::ZERO {
                    return Err(ProcessorError::InvalidAmount {
                        message: format!("invalid amount for tx_id={}", tx.id),
                    });
                }
            }

            // Find the wallet actor to route this transaction to. All transactions from a client
            // will always go to the same WalletActor, so that, the client always has a single and
            // complete state in the system.
            let wallet_actor = self.wallet_actors.get(tx.client as usize % self.actor_count).unwrap();

            // Sending WalletActor the transaction
            if let Err(e) = wallet_actor.tell(WalletActorMessages::Tx(tx)).await {
                eprintln!("Channel Full, increase buffer size and run the test again {}", e);
                return Err(ProcessorError::FatalError);
            }
        }

        Ok(())
    }

    pub async fn output<W>(&mut self, mut stream: CsvStreamWriter<W>) -> ProcessorResult<()>
    where
        W: std::io::Write,
    {
        for actor in self.wallet_actors.iter() {
            let (tx, rx) = oneshot::channel();

            // Sending command to fetch all the wallets from a WalletActor
            if let Ok(wallet_state) = actor.ask(WalletActorMessages::Output(tx), rx).await {
                for wallet in wallet_state {
                    let wallet_csv_view: WalletCsvView = wallet.into();

                    stream.writer.serialize(wallet_csv_view).map_err(|e| {
                        eprintln!("SERDE ERROR: {:?}", e);
                        ProcessorError::Serialization(e.to_string())
                    })?;
                }
            }
        }

        stream
            .writer
            .flush()
            .map_err(|e| ProcessorError::Serialization(e.to_string()))?;

        Ok(())
    }
}
