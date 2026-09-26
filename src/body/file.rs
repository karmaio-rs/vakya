use super::{Body, Frame, ReaderBody, SizeHint, TrailerHint};
use bytes::BytesMut;
use karmaio::{
    buf::{BufResult, IoBufMut},
    fs::File,
    io::{AsyncRead, AsyncReadAt},
};
use std::io;

/// A supplied file streamed positionally within its metadata length snapshot.
///
/// Reads start at offset zero and use bounded 16 KiB chunks, independent of the
/// file descriptor's seek position. Exact remaining length is advertised and
/// trailers are absent. Growth beyond the initial snapshot is ignored; EOF
/// before that boundary is `UnexpectedEof`. This is a length snapshot, not a
/// snapshot of file contents: concurrent writes may change bytes within it.
/// Frame waits retain the same owned-reader operation machinery as [`ReaderBody`].
/// File acquisition, paths, routing, permissions, MIME types, and ranges remain
/// application policy. Reading and metadata require an active Karmaio runtime.
pub struct FileBody {
    reader: ReaderBody<PositionalFile>,
    snapshot_len: u64,
    remaining: u64,
    done: bool,
}

/// A file recovered after cancellation and settlement of its retained read.
pub struct FileParts {
    /// The originally supplied file, with its descriptor seek position untouched.
    pub file: File,
    /// Initial metadata length used as the body boundary.
    pub snapshot_len: u64,
    /// Next positional offset after successful reads, including raced completion.
    pub offset: u64,
    /// The pending read's initialized bytes, or an empty idle cache.
    pub buffer: BytesMut,
    /// Validated pending-read result, or `None` when no operation was retained.
    pub result: Option<io::Result<usize>>,
}

impl FileBody {
    /// Snapshot a supplied file's metadata length and stream from offset zero.
    ///
    /// # Errors
    /// Returns the original metadata error. On failure the supplied file is dropped.
    pub async fn new(file: File) -> io::Result<Self> {
        let snapshot_len = file.metadata().await?.len();
        Ok(Self {
            reader: ReaderBody::new(PositionalFile { file, offset: 0 }),
            snapshot_len,
            remaining: snapshot_len,
            done: false,
        })
    }

    /// Cancel and settle a retained positional read and recover the supplied file.
    /// A raced successful completion advances `offset` and preserves its bytes.
    /// No further read, drain, sync, or close operation is performed. Keep driving
    /// this future until it returns to certify settlement.
    pub async fn into_parts(self) -> FileParts {
        let parts = self.reader.into_parts().await;
        FileParts {
            file: parts.reader.file,
            offset: parts.reader.offset,
            snapshot_len: self.snapshot_len,
            buffer: parts.buffer,
            result: parts.result,
        }
    }
}
impl Body for FileBody {
    type Data = BytesMut;
    type Error = io::Error;
    async fn next_frame(&mut self) -> io::Result<Option<Frame<BytesMut>>> {
        if self.done {
            return Ok(None);
        }
        if self.remaining == 0 {
            self.done = true;
            return Ok(None);
        }
        let width = self.remaining.min(16 * 1024) as usize;
        match self.reader.next_chunk(width).await {
            Ok(Some(buffer)) => {
                self.remaining -= buffer.len() as u64;
                Ok(Some(Frame::data(buffer)))
            }
            Ok(None) => {
                self.done = true;
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file ended before its body length snapshot",
                ))
            }
            Err(error) => {
                self.done = true;
                Err(error)
            }
        }
    }
    #[inline]
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(if self.done { 0 } else { self.remaining })
    }
    #[inline]
    fn trailer_hint(&self) -> TrailerHint {
        TrailerHint::None
    }
    #[inline]
    fn is_end_stream(&self) -> bool {
        self.done || self.remaining == 0
    }
    #[inline]
    fn recycle(&mut self, data: BytesMut) {
        self.reader.recycle(data);
    }
}
struct PositionalFile {
    file: File,
    offset: u64,
}
impl AsyncRead for PositionalFile {
    async fn read<B: IoBufMut>(&mut self, buffer: B) -> BufResult<usize, B> {
        let (result, buffer) = self.file.read_at(buffer, self.offset).await.into_parts();
        let result = result.and_then(|count| {
            self.offset = self
                .offset
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "file body offset overflow"))?;
            Ok(count)
        });
        BufResult(result, buffer)
    }
}
