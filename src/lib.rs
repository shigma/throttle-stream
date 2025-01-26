use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use std::sync::Arc;

pub struct ThrottleStream<T> {
    duration: Duration,
    buffer: Arc<Mutex<Vec<T>>>,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: mpsc::Sender<Vec<T>>,
}

impl<T: Send + 'static> ThrottleStream<T> {
    pub fn clone(&self) -> Self {
        Self {
            duration: self.duration,
            buffer: self.buffer.clone(),
            task: self.task.clone(),
            sender: self.sender.clone(),
        }
    }

    pub fn new(duration: Duration) -> (Self, mpsc::Receiver<Vec<T>>) {
        let (sender, receiver) = mpsc::channel(1);
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let task = Arc::new(Mutex::new(None));
        (
            Self {
                duration,
                buffer,
                task,
                sender,
            },
            receiver,
        )
    }

    async fn send_data(&self, data: Vec<T>, task_guard: &mut Option<JoinHandle<()>>) {
        if let Err(e) = self.sender.send(data).await {
            eprintln!("Failed to send data: {}", e);
            return;
        }
        *task_guard = Some(self.schedule_task());
    }

    fn schedule_task(&self) -> JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            sleep(this.duration).await;

            let mut buffer_guard = this.buffer.lock().await;
            let mut task_guard = this.task.lock().await;
            if buffer_guard.is_empty() {
                *task_guard = None;
                return;
            }

            let data = buffer_guard.drain(..).collect::<Vec<_>>();
            this.send_data(data, &mut task_guard).await;
        })
    }

    pub async fn push(&self, item: T) {
        let mut task_guard = self.task.lock().await;
        if task_guard.is_some() {
            let mut buffer_guard = self.buffer.lock().await;
            buffer_guard.push(item);
        } else {
            self.send_data(vec![item], &mut task_guard).await;
        }
    }

    pub async fn flush(&self) {
        let mut buffer_guard = self.buffer.lock().await;
        let mut task_guard = self.task.lock().await;
        if buffer_guard.is_empty() {
            return;
        }

        let data = buffer_guard.drain(..).collect::<Vec<_>>();
        self.send_data(data, &mut task_guard).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_throttle_stream() {
        let (throttle, mut receiver) = ThrottleStream::new(Duration::from_millis(100));

        throttle.push(1).await;
        // time: 0ms
        assert_eq!(receiver.try_recv(), Ok(vec![1]));

        throttle.push(2).await;
        throttle.push(3).await;
        // time: 0ms
        assert!(receiver.try_recv().is_err());

        sleep(Duration::from_millis(120)).await;
        // time: 120ms
        assert_eq!(receiver.try_recv(), Ok(vec![2, 3]));

        throttle.push(4).await;
        throttle.push(5).await;
        // time: 120ms
        assert!(receiver.try_recv().is_err());

        sleep(Duration::from_millis(120)).await;
        // time: 240ms
        assert_eq!(receiver.recv().await, Some(vec![4, 5]));
    }

    #[tokio::test]
    async fn test_throttle_stream_flush() {
        let (throttle, mut receiver) = ThrottleStream::new(Duration::from_millis(100));

        throttle.push(1).await;
        // time: 0ms
        assert_eq!(receiver.try_recv(), Ok(vec![1]));

        throttle.push(2).await;
        throttle.push(3).await;
        // time: 0ms
        assert!(receiver.try_recv().is_err());

        throttle.flush().await;
        // time: 0ms
        assert_eq!(receiver.try_recv(), Ok(vec![2, 3]));

        throttle.flush().await;
        // time: 0ms
        assert!(receiver.try_recv().is_err());

        throttle.push(4).await;
        throttle.push(5).await;
        // time: 0ms
        assert!(receiver.try_recv().is_err());

        throttle.flush().await;
        // time: 0ms
        assert_eq!(receiver.try_recv(), Ok(vec![4, 5]));
    }
}
