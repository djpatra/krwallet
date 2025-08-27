use tokio::sync::{mpsc, oneshot};

use crate::{map_channel_recv_err, map_channel_send_err, ProcessorError, ProcessorResult};

#[derive(Debug)]
pub struct ActorRef<M>
where
    M: Send,
{
    sender: mpsc::Sender<M>,
}

impl<M: Send + 'static> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<M: Send> ActorRef<M> {
    /// Fire-and-forget send. 
    pub async fn tell(&self, msg: M) -> ProcessorResult<()> {
        self.sender.send(msg).await.map_err(map_channel_send_err)
    }

    /// Ask pattern: send a message and wait for a response.
    /// The message type `M` should contain an `oneshot::Sender<R>`.
    pub async fn ask<R>(&self, msg: M, response_channel: oneshot::Receiver<R>) -> ProcessorResult<R> 
    where
        R: Send + 'static,
    {
        self.tell(msg).await?;
        response_channel.await.map_err(map_channel_recv_err)
    }
}

/// Actors implement this trait for their message type.
#[async_trait::async_trait]
pub trait ChannelActor<M>
where
    M: Send,
{
    async fn handle(&mut self, msg: M) -> ProcessorResult<()>;
}

/// Start an actor with bounded buffer size.
/// Spawns the actor loop on a tokio task and returns an `ActorRef`.
pub async fn start<A, M>(mut actor_instance: A, buf_size: usize) -> ActorRef<M>
where
    M: Send + 'static,
    A: ChannelActor<M> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(buf_size);
    let actor_ref = ActorRef { sender: tx };

    let _join_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(err) = actor_instance.handle(msg).await {
                // Not the right way to kill an actor. Ideally, we should have 
                // an explicit PoisonPill sent to self and then exit
                match err {
                    ProcessorError::FatalError => {
                        break;
                    },
                    _ => {
                        // Commenting out this so that tests do not fail; 
                        // Should log errors to file
                        // eprintln!("Actor error: {:?}", err);
                    }
                }
            }
        }
    });

    actor_ref
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;
    use std::sync::{Arc, Mutex};

    // simple test actor
    struct TestActor {
        state: Arc<Mutex<Vec<i32>>>,
    }

    #[derive(Debug)]
    enum TestMessage {
        Store(i32),
        Get(oneshot::Sender<Vec<i32>>),
    }

    #[async_trait::async_trait]
    impl ChannelActor<TestMessage> for TestActor {
        async fn handle(&mut self, msg: TestMessage) -> ProcessorResult<()> {
            match msg {
                TestMessage::Store(v) => {
                    self.state.lock().unwrap().push(v);
                }
                TestMessage::Get(tx) => {
                    let snapshot = self.state.lock().unwrap().clone();
                    let _ = tx.send(snapshot);
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_tell_updates_state() {
        let state = Arc::new(Mutex::new(Vec::new()));
        let actor = start(
            TestActor { state: state.clone() },
            8
        ).await;

        actor.tell(TestMessage::Store(10)).await.unwrap();
        actor.tell(TestMessage::Store(20)).await.unwrap();

        // give task a chance to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let snapshot = state.lock().unwrap().clone();
        assert_eq!(snapshot, vec![10, 20]);
    }

    #[tokio::test]
    async fn test_ask_returns_state() {
        let state = Arc::new(Mutex::new(Vec::new()));
        let actor = start(
            TestActor { state: state.clone() },
            8
        ).await;

        actor.tell(TestMessage::Store(42)).await.unwrap();

        let (tx, rx) = oneshot::channel();
        actor.ask(TestMessage::Get(tx), rx).await
            .map(|result| assert_eq!(result, vec![42]))
            .unwrap();
    }
}

