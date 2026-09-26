#![cfg(feature = "fs")]
use std::{
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use vakya::body::{Body, BodyExt, FileBody, TrailerHint};

struct Fixture(PathBuf);
impl Fixture {
    fn new(bytes: &[u8]) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "vakya-file-body-{}-{stamp}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_file(&self.0).unwrap();
    }
}

#[test]
fn file_snapshot_zero_partial_chunks_and_growth_preserve_exact_remaining_length() {
    karmaio::Runtime::new().unwrap().block_on(async {
        for length in [0, 1, 16 * 1024 + 3] {
            let expected: Vec<u8> = (0..length).map(|index| (index % 251) as u8).collect();
            let fixture = Fixture::new(&expected);
            let file = karmaio::fs::File::open(&fixture.0).await.unwrap();
            let mut body = FileBody::new(file).await.unwrap();
            assert_eq!(body.size_hint().exact(), Some(length as u64));
            assert_eq!(body.trailer_hint(), TrailerHint::None);
            std::fs::OpenOptions::new()
                .append(true)
                .open(&fixture.0)
                .unwrap()
                .write_all(b"later growth")
                .unwrap();
            let mut received = Vec::new();
            while let Some(frame) = body.next_frame().await.unwrap() {
                let bytes = frame.into_data().unwrap();
                assert!(bytes.len() <= 16 * 1024);
                received.extend_from_slice(&bytes);
                assert_eq!(body.size_hint().exact(), Some((length - received.len()) as u64));
                body.recycle(bytes);
            }
            assert_eq!(received, expected);
            assert!(body.is_end_stream());
            assert!(body.next_frame().await.unwrap().is_none());
            let parts = body.into_parts().await;
            assert_eq!(parts.snapshot_len, length as u64);
            assert_eq!(parts.offset, length as u64);
            assert!(parts.result.is_none());
        }
    });
}

#[test]
fn file_shrinkage_reports_premature_eof_and_remains_terminal() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new(b"snapshot");
        let file = karmaio::fs::File::open(&fixture.0).await.unwrap();
        let mut body = FileBody::new(file).await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fixture.0)
            .unwrap()
            .set_len(2)
            .unwrap();
        let frame = body.next_frame().await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(&frame[..], b"sn");
        assert_eq!(body.size_hint().exact(), Some(6));
        body.recycle(frame);
        assert_eq!(
            body.next_frame().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert!(body.next_frame().await.unwrap().is_none());
        let parts = body.into_parts().await;
        assert_eq!(parts.offset, 2);
        assert_eq!(parts.snapshot_len, 8);
    });
}

#[test]
fn supplied_file_reads_start_at_zero_without_moving_the_descriptor_cursor() {
    karmaio::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new(b"contents");
        let mut file = std::fs::File::open(&fixture.0).unwrap();
        file.seek(SeekFrom::Start(3)).unwrap();
        let mut observer = file.try_clone().unwrap();
        let file = karmaio::fs::File::from_std(file).unwrap();
        let body = FileBody::new(file).await.unwrap();
        assert_eq!(body.collect(8).await.unwrap().bytes().as_ref(), b"contents");
        assert_eq!(observer.stream_position().unwrap(), 3);
    });
}

#[test]
fn file_decomposition_preserves_a_pending_or_raced_read_without_further_io() {
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    karmaio::Runtime::new().unwrap().block_on(async {
        let fixture = Fixture::new(b"pending file bytes");
        let file = karmaio::fs::File::open(&fixture.0).await.unwrap();
        let mut body = FileBody::new(file).await.unwrap();
        let delivered = {
            let mut wait = pin!(body.next_frame());
            match wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Pending => None,
                Poll::Ready(result) => result.unwrap().map(|frame| frame.into_data().unwrap()),
            }
        };
        let parts = body.into_parts().await;
        assert_eq!(parts.snapshot_len, 18);
        match parts.result {
            Some(Ok(count)) => {
                assert_eq!(count, parts.buffer.len());
                assert_eq!(parts.offset, count as u64);
                assert_eq!(&parts.buffer[..], &b"pending file bytes"[..count]);
            }
            Some(Err(error)) => {
                assert!(karmaio::runtime::is_operation_canceled(&error));
                assert_eq!(parts.offset, 0);
            }
            None => {
                assert_eq!(&delivered.unwrap()[..], b"pending file bytes");
                assert_eq!(parts.offset, 18);
            }
        }
        assert_eq!(parts.file.metadata().await.unwrap().len(), 18);
    });
}
