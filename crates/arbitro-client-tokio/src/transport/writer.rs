//! Single-writer transport task — generic over `AsyncWrite`, drains the
//! kit Mpsc consumer with `recv_async` + `try_recv`.

use arbitro_kit::route::MpscAsyncConsumer;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::transport::frame::{WriteFrame, WRITE_QUEUE_CAP};

/// Writer unit tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::frame::{WriteFrame, INLINE_CAP, MAX_WRITE_PRODUCERS};
    use arbitro_kit::route::MpscAsync;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// Three inline frames arrive at the reader in the same order they were enqueued.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_writer_drains_in_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (mut producers, mut consumer, _shutdown) =
            MpscAsync::<WriteFrame, WRITE_QUEUE_CAP>::new(MAX_WRITE_PRODUCERS);
        let producer = producers.remove(0);

        // Enqueue 3 fixed payloads (pad inline arrays with zeros after content).
        let chunks: &[&[u8]] = &[b"aaa", b"bbbbb", b"cc"];
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        for chunk in chunks {
            let mut data = [0u8; INLINE_CAP];
            data[..chunk.len()].copy_from_slice(chunk);
            producer
                .try_send(WriteFrame::Inline(data, chunk.len() as u16))
                .unwrap();
        }

        // Accept the write side.
        let accept_h = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_, write_half) = client.into_split();
        let mut server_read = accept_h.await.unwrap();

        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            writer_task(&mut consumer, write_half, cancel2).await.ok();
        });

        let mut buf = vec![0u8; total];
        server_read.read_exact(&mut buf).await.unwrap();
        cancel.cancel();

        assert_eq!(&buf[0..3], b"aaa");
        assert_eq!(&buf[3..8], b"bbbbb");
        assert_eq!(&buf[8..10], b"cc");
    }
}

pub(crate) async fn writer_task<W: AsyncWrite + Unpin>(
    consumer: &mut MpscAsyncConsumer<WriteFrame, WRITE_QUEUE_CAP>,
    mut w: W,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = consumer.recv_async() => {
                let Ok(frame) = result else {
                    let _ = w.shutdown().await;
                    return Ok(());
                };
                w.write_all(frame.as_slice()).await?;
                // drain any frames that arrived while we were writing
                while let Some(f) = consumer.try_recv() {
                    w.write_all(f.as_slice()).await?;
                }
            }
        }
    }
}
