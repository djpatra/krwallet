use rust_decimal::Decimal;

use crate::{channel_actor::{self, ActorRef}, CsvStream, ProcessorError, ProcessorResult, Transaction, TransactionType};

use super::wallet_actor::{WalletActor, WalletActorMessages};

pub struct TransactionProcessor {
    actor_count: usize,
    wallet_actors: Vec<ActorRef<WalletActorMessages>>,
}


impl TransactionProcessor {
    pub async fn new(actor_count: usize, channel_buffer_size: usize) -> Self {
        let mut wallet_actors = Vec::with_capacity(actor_count);
        for _ in 0..actor_count {
            let actor = WalletActor::create();
            let actor_ref = channel_actor::start(actor, channel_buffer_size).await;
            wallet_actors.push(actor_ref);
        }

        Self {
            actor_count, 
            wallet_actors 
        }
    }
    
    pub async fn process<R>(&mut self, mut stream: CsvStream<R>) -> ProcessorResult<()>
    where
        R: std::io::Read,
    {
        for result in stream.reader.deserialize::<Transaction>() {
            let tx = match result {
                Ok(transaction) => transaction,
                Err(e) => {
                    eprintln!("Error deserializing record: {}", e);
                    continue;
                }
            };

            // Validate amount for Deposits and Withdrawl. This validation also ensures
            // that we can safely unwrap amount out of the Option
            if matches!(tx.tx_type, TransactionType::Deposit | TransactionType::Withdrawal) {
                let amount = tx.amount.ok_or(ProcessorError::InvalidAmount {
                    message: format!("invalid amount for tx_id={}", tx.id)
                })?;

                if amount < Decimal::ZERO {
                    return Err(ProcessorError::InvalidAmount {
                        message: format!("invalid amount for tx_id={}", tx.id)
                    })    
                }
            }

            let wallet_actor = self.wallet_actors.get(tx.client as usize % self.actor_count).unwrap();
            if let Err(e) = wallet_actor.tell(WalletActorMessages::Tx(tx)).await {
                eprintln!("Channel Full, increase buffer size and run the test again {}", e);
                break;
            }
        }
        
        Ok(())
    }
}    
