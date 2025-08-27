use std::collections::HashMap;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use crate::{channel_actor::ChannelActor, ProcessorError, ProcessorResult, Transaction, TransactionType};

#[derive(Debug)]
pub(crate) enum WalletActorMessages {
    Tx(Transaction),
    Output(oneshot::Sender<Vec<(u16, Wallet)>>),
}

#[derive(Clone, Default, Debug)]
pub struct Wallet {
    available: Decimal,
    held: Decimal,
    // Store transaction history for disputes
    transactions: HashMap<u32, Transaction>,
    locked: bool,
}

impl Wallet {
    pub fn process_transaction(&mut self, tx: Transaction) -> ProcessorResult<()> {
        if self.locked && matches!(tx.tx_type, TransactionType::Deposit | TransactionType::Withdrawal) {
            return Err(ProcessorError::AccountLocked { client_id: tx.client })
        }

        match tx.tx_type {
            TransactionType::Deposit => self.handle_deposit(tx),
            TransactionType::Withdrawal => self.handle_withdrawl(tx),
            TransactionType::Dispute => self.handle_dispute(tx.id),
            TransactionType::Resolve => self.handle_resolve(tx.id),
            TransactionType::Chargeback => self.handle_chargeback(tx.id),
        }
    }

    fn handle_deposit(&mut self, tx: Transaction) -> ProcessorResult<()> {        
        if self.transactions.contains_key(&tx.id) {
            return Err(ProcessorError::DuplicateTransaction { tx_id: tx.id });
        }
        
        self.available += tx.amount.unwrap();
        self.transactions.insert(tx.id, tx);
        Ok(())
    }

    
    fn handle_withdrawl(&mut self, tx: Transaction) -> ProcessorResult<()> {        
        if self.transactions.contains_key(&tx.id) {
            return Err(ProcessorError::DuplicateTransaction { tx_id: tx.id });
        }

        let amount = tx.amount.unwrap();
        
        if self.available < amount {
            return Err(ProcessorError::InsufficientFunds { available: self.available, required: amount });
        }
        
        self.available -= amount;
        self.transactions.insert(tx.id, tx);
        Ok(())
    }


    fn handle_dispute(&mut self, tx_id: u32) -> ProcessorResult<()> {
        let tx = self.transactions.get_mut(&tx_id)
            .ok_or(ProcessorError::TransactionNotFound { tx_id })?;

        tx.disputed = true;
        let amount = tx.amount.unwrap();
        
        match tx.tx_type {
            TransactionType::Deposit => {
                // We are allowing negative wallet balance
                self.available -= amount;
                self.held += amount;
            }
            TransactionType::Withdrawal => {
                self.held += amount;
            }
            _ => {} // NoOp, as we keep track of deposits and withdrawls only
        }
        
        Ok(())
    }

    fn handle_resolve(&mut self, tx_id: u32) -> ProcessorResult<()> {
        let tx = self.transactions.get_mut(&tx_id)
            .ok_or(ProcessorError::TransactionNotFound { tx_id })?;
        
        if !tx.disputed {
            return Ok(()); // Ignore if not disputed
        }

        tx.disputed = false;
        // Safe unwrap as validation done earlier in Processor
        let amount = tx.amount.unwrap();
        
        match tx.tx_type {
            TransactionType::Deposit => {
                self.held -= amount;
                self.available += amount;
            }
            TransactionType::Withdrawal => {
                self.held -= amount;
            }
            _ => {} // NoOp, as we keep track of deposits and withdrawls only
        }
        
        Ok(())
    }
    
    fn handle_chargeback(&mut self, tx_id: u32) -> ProcessorResult<()> {
        let tx = self.transactions.get_mut(&tx_id)
            .ok_or(ProcessorError::TransactionNotFound { tx_id })?;
        
        if !tx.disputed {
            return Ok(()); // Ignore if not disputed
        }

        tx.disputed = false;
        // Safe unwrap as validation done earlier in Processor        
        let amount = tx.amount.unwrap();
        
        match tx.tx_type {
            TransactionType::Deposit => {
                self.held -= amount;
                self.locked = true;
            }
            TransactionType::Withdrawal => {
                self.held -= amount;
                self.available += amount;
                self.locked = true;
            }
            _ => {}
        }
        
        Ok(())
    }
}    


pub(crate) struct WalletActor {
     wallets: HashMap<u16, Wallet>,
}

impl WalletActor {
    pub(crate) fn create() -> Self {
        Self {
            wallets: HashMap::new()
        }
    }
}

#[async_trait::async_trait]
impl ChannelActor<WalletActorMessages> for WalletActor {
    async fn handle(&mut self, msg: WalletActorMessages) -> ProcessorResult<()> {
        use WalletActorMessages::*;
        
        match msg {
            Tx(tx) => {    
                let wallet = self.wallets.entry(tx.client)
                .or_insert_with(|| Wallet::default());

                let tx_id = tx.id;
                if let Err(e) = wallet.process_transaction(tx) {
                    eprintln!("Error processing transaction {}: {}", tx_id, e);
                }                
            },

            Output(sender) => {
                let state: Vec<(u16, Wallet)> = std::mem::take(&mut self.wallets).into_iter().collect();
                let _ = sender.send(state);
            }
        }

        Ok(())
    }
}
