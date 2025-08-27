**Overview**

This is an actor-based wallet service that manages client accounts and their transactions concurrently. The design enables independent processing of multiple transaction types, including deposits, withdrawals, disputes, resolves, and chargebacks, for multiple clients simultaneously. The Transaction Processor establishes a `many-to-one` relationship between clients and WalletActors: each client is mapped to a single WalletActor, while a WalletActor can handle multiple clients. Each WalletActor maintains the wallet state for its assigned clients and is responsible for applying all transaction-induced mutations to their wallets.


**Key design goals**

Threadsafe handling of multiple clients

High performance, asynchronous transaction processing

Separation of concerns using actors

Deterministic and testable transaction outcomes


**Architecture**

           ┌─────────────────┐
           │ Transaction CSV │
           └────────┬────────┘
                    │
                    ▼
          ┌─────────────────────┐________> Csv Output to Stdout 
          │ Transaction Processor│    
          └────────┬────────────┘ <_______       
                   │                     | Wallets for all
                   ▼                     | Clients
         ┌────────────────────┐          |
         │  Wallet Actor Pool  │         |
         │ ┌───────────────┐  │          |
         │ │ WalletActor #1│  │          |
         │ └───────────────┘  │__________|
         │ ┌───────────────┐  │
         │ │ WalletActor #2│  │
         │ └───────────────┘  │
         └────────────────────┘

**Input**

type,client,tx,amount

deposit, 1, 1, 1.0

deposit, 2, 2, 2.0

deposit, 1, 3, 2.0

withdrawal, 1, 4, 1.5

withdrawal, 2, 5, 3.0


**Output**

client_id,available,held,total,locked

1,1.5000,0.0000,1.5000,false

2,2.0000,0.0000,2.0000,false

