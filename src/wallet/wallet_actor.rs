use std::collections::HashMap;
use rust_decimal::Decimal;
use tokio::sync::oneshot;

use crate::{channel_actor::ChannelActor, ProcessorError, ProcessorResult, Transaction, TransactionType};

#[derive(Debug)]
pub(crate) enum WalletActorMessages {
    Tx(Transaction),
    Output(oneshot::Sender<Vec<WalletState>>),
}

#[derive(Clone, Default, Debug)]
pub struct Wallet {
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
    // Store transaction history for disputes
    pub transactions: HashMap<u32, Transaction>,
}

#[derive(Debug)]
pub(crate) struct  WalletState {
    pub client_id: u16,
    pub wallet: Wallet,
}


impl Wallet {
    pub fn process_transaction(&mut self, tx: Transaction) -> ProcessorResult<()> {
        // If the wallet is locked, then no deposits and withdrawals are allowed
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

        // Safe unwrap as validation done earlier in Processor        
        self.available += tx.amount.unwrap();

        self.transactions.insert(tx.id, tx);
        Ok(())
    }

    
    fn handle_withdrawl(&mut self, tx: Transaction) -> ProcessorResult<()> {        
        if self.transactions.contains_key(&tx.id) {
            return Err(ProcessorError::DuplicateTransaction { tx_id: tx.id });
        }

        // Safe unwrap as validation done earlier in Processor        
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
        // Safe unwrap as validation done earlier in Processor        
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
            return Err(ProcessorError::InvalidDisputeState) // Ignore if not disputed
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
            return Err(ProcessorError::InvalidDisputeState) // Ignore if not disputed
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

                wallet.process_transaction(tx)?;
            },

            Output(sender) => {
                let state: Vec<WalletState> = std::mem::take(&mut self.wallets).into_iter()
                    .map(|(client_id, mut wallet)| {
                        wallet.total = wallet.available + wallet.held;
                        WalletState { client_id, wallet }
                    }).collect();
                let _ = sender.send(state);
            }
        }

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::{prelude::FromPrimitive, Decimal};

    fn make_tx(id: u32, client: u16, tx_type: TransactionType, amount: Option<Decimal>) -> Transaction {
        Transaction {
            id,
            client,
            tx_type,
            amount,
            disputed: false,
        }
    }

    #[test]
    fn deposit_increases_balance_and_records_tx() {
        let mut wallet = Wallet::default();
        let tx = make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(50.0));

        wallet.process_transaction(tx.clone()).unwrap();

        assert_eq!(Some(wallet.available), Decimal::from_f32(50.0));
        assert!(wallet.transactions.contains_key(&1));
    }

    #[test]
    fn duplicate_deposit_returns_error() {
        let mut wallet = Wallet::default();
        let tx = make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(10.0));

        wallet.process_transaction(tx.clone()).unwrap();
        let err = wallet.process_transaction(tx.clone()).unwrap_err();

        match err {
            ProcessorError::DuplicateTransaction { tx_id } => assert_eq!(tx_id, 1),
            _ => panic!("Expected DuplicateTransaction error"),
        }
    }

    #[test]
    fn withdrawal_reduces_balance() {
        let mut wallet = Wallet::default();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(100.0))).unwrap();
        wallet.process_transaction(make_tx(2, 100, TransactionType::Withdrawal, Decimal::from_f32(30.0))).unwrap();

        assert_eq!(Some(wallet.available), Decimal::from_f32(70.0));
    }

    #[test]
    fn withdrawal_insufficient_funds_fails() {
        let mut wallet = Wallet::default();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(20.0))).unwrap();

        let err = wallet.process_transaction(make_tx(2, 100, TransactionType::Withdrawal, Decimal::from_f32(50.0))).unwrap_err();

        match err {
            ProcessorError::InsufficientFunds { available, required } => {
                assert_eq!(Some(available), Decimal::from_f32(20.0));
                assert_eq!(Some(required), Decimal::from_f32(50.0));
            }
            _ => panic!("Expected InsufficientFunds error"),
        }
    }

    #[test]
    fn dispute_moves_funds_to_held() {
        let mut wallet = Wallet::default();
        let deposit = make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(100.0));
        wallet.process_transaction(deposit).unwrap();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Dispute, None)).unwrap();

        assert_eq!(Some(wallet.available), Decimal::from_f32(0.0));
        assert_eq!(Some(wallet.held), Decimal::from_f32(100.0));
    }

    #[test]
    fn resolve_moves_back_from_held() {
        let mut wallet = Wallet::default();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(100.0))).unwrap();
        wallet.process_transaction(make_tx(2, 100, TransactionType::Deposit, Decimal::from_f32(100.0))).unwrap();
        wallet.process_transaction(make_tx(2, 100, TransactionType::Dispute, None)).unwrap();
        wallet.process_transaction(make_tx(2, 100, TransactionType::Resolve, None)).unwrap();

        assert_eq!(Some(wallet.available), Decimal::from_f32(200.0));
        assert_eq!(Some(wallet.held), Decimal::from_f32(0.0));
    }

    #[test]
    fn chargeback_locks_account_and_removes_funds() {
        let mut wallet = Wallet::default();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(200.0))).unwrap();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Dispute, None)).unwrap();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Chargeback, None)).unwrap();

        assert!(wallet.locked);
        assert_eq!(Some(wallet.available), Decimal::from_f32(0.0));
        assert_eq!(Some(wallet.held), Decimal::from_f32(0.0));
    }

    #[test]
    fn locked_account_rejects_new_deposits_and_withdrawals() {
        let mut wallet = Wallet::default();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(100.0))).unwrap();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Dispute, None)).unwrap();
        wallet.process_transaction(make_tx(1, 100, TransactionType::Chargeback, None)).unwrap();

        let deposit_err = wallet.process_transaction(make_tx(1, 100, TransactionType::Deposit, Decimal::from_f32(50.0))).unwrap_err();
        assert!(matches!(deposit_err, ProcessorError::AccountLocked { .. }));

        let withdrawal_err = wallet.process_transaction(make_tx(5, 100, TransactionType::Withdrawal, Decimal::from_f32(10.0))).unwrap_err();
        assert!(matches!(withdrawal_err, ProcessorError::AccountLocked { .. }));
    }
}
