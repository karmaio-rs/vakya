//! Deterministic completion transport shared by internal and integration tests.
//! Gates separate operation submission, cancellation observation, and completion.
use bytes::Bytes;
use karmaio::{
    buf::{BufResult, IoBuf, IoBufMut, IoVectoredBuf},
    io::{AsyncRead, AsyncWrite, IntoOwnedSplit},
    runtime::CancellationToken,
};
use std::{
    cell::RefCell,
    collections::VecDeque,
    future::poll_fn,
    io,
    rc::Rc,
    task::{Poll, Waker},
};

#[derive(Clone, Default)]
pub struct Gate(Rc<RefCell<GateState>>);

#[derive(Default)]
struct GateState {
    open: bool,
    waker: Option<Waker>,
}

impl Gate {
    pub fn open(&self) {
        let waker = {
            let mut state = self.0.borrow_mut();
            state.open = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub fn is_open(&self) -> bool {
        self.0.borrow().open
    }

    pub async fn wait(&self) {
        poll_fn(|cx| {
            let mut state = self.0.borrow_mut();
            if state.open {
                Poll::Ready(())
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

pub enum ReadStep {
    Data(Bytes),
    Error(io::Error),
    Wait(Gate),
    AfterCancel {
        token: CancellationToken,
        observed: Gate,
        complete: Gate,
    },
}

pub struct Reader {
    steps: VecDeque<ReadStep>,
    pub submissions: usize,
    pub capacities: Vec<usize>,
}

impl Reader {
    pub fn new(steps: impl IntoIterator<Item = ReadStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            submissions: 0,
            capacities: Vec::new(),
        }
    }
}

impl AsyncRead for Reader {
    async fn read<B: IoBufMut>(&mut self, mut buffer: B) -> BufResult<usize, B> {
        self.submissions += 1;
        self.capacities.push(buffer.as_uninit().len());
        loop {
            match self.steps.pop_front() {
                Some(ReadStep::Wait(gate)) => gate.wait().await,
                Some(ReadStep::AfterCancel {
                    token,
                    observed,
                    complete,
                }) => {
                    token.cancelled().await;
                    observed.open();
                    complete.wait().await;
                }
                Some(ReadStep::Error(error)) => return BufResult(Err(error), buffer),
                Some(ReadStep::Data(mut bytes)) => {
                    let count = bytes.len().min(buffer.as_uninit().len());
                    for (slot, byte) in buffer.as_uninit()[..count].iter_mut().zip(&bytes[..count]) {
                        slot.write(*byte);
                    }
                    // SAFETY: exactly this prefix has just been initialized.
                    unsafe { buffer.set_len(count) };
                    if count < bytes.len() {
                        let _ = bytes.split_to(count);
                        self.steps.push_front(ReadStep::Data(bytes));
                    }
                    return BufResult(Ok(count), buffer);
                }
                None => return BufResult(Ok(0), buffer),
            }
        }
    }
}

pub enum WriteStep {
    Limit(usize),
    Error(io::Error),
    Wait(Gate),
    AfterCancel {
        token: CancellationToken,
        observed: Gate,
        complete: Gate,
    },
    /// Deliberately invalid count for contract-validation tests.
    Report(usize),
}

pub struct Writer {
    steps: VecDeque<WriteStep>,
    default_limit: usize,
    pub output: Vec<u8>,
    pub submissions: usize,
    pub pointers: Vec<usize>,
}

impl Writer {
    pub fn new(steps: impl IntoIterator<Item = WriteStep>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            default_limit: usize::MAX,
            output: Vec::new(),
            submissions: 0,
            pointers: Vec::new(),
        }
    }

    pub fn limited(width: usize) -> Self {
        let mut writer = Self::new([]);
        writer.default_limit = width;
        writer
    }

    async fn next(&mut self) -> io::Result<(usize, bool)> {
        self.submissions += 1;
        loop {
            match self.steps.pop_front() {
                Some(WriteStep::Wait(gate)) => gate.wait().await,
                Some(WriteStep::AfterCancel {
                    token,
                    observed,
                    complete,
                }) => {
                    token.cancelled().await;
                    observed.open();
                    complete.wait().await;
                }
                Some(WriteStep::Limit(count)) => return Ok((count, false)),
                Some(WriteStep::Error(error)) => return Err(error),
                Some(WriteStep::Report(count)) => return Ok((count, true)),
                None => return Ok((self.default_limit, false)),
            }
        }
    }
}

impl AsyncWrite for Writer {
    async fn write<B: IoBuf>(&mut self, buffer: B) -> BufResult<usize, B> {
        let result = match self.next().await {
            Ok((count, true)) => Ok(count),
            Ok((limit, false)) => {
                let bytes = buffer.as_init();
                self.pointers.push(bytes.as_ptr() as usize);
                let count = limit.min(bytes.len());
                self.output.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            Err(error) => Err(error),
        };
        BufResult(result, buffer)
    }

    async fn write_vectored<V: IoVectoredBuf>(&mut self, buffers: V) -> BufResult<usize, V> {
        let result = match self.next().await {
            Ok((count, true)) => Ok(count),
            Ok((mut limit, false)) => {
                let mut written = 0;
                for bytes in buffers.iter_slice() {
                    self.pointers.push(bytes.as_ptr() as usize);
                    let count = limit.min(bytes.len());
                    self.output.extend_from_slice(&bytes[..count]);
                    limit -= count;
                    written += count;
                    if limit == 0 {
                        break;
                    }
                }
                Ok(written)
            }
            Err(error) => Err(error),
        };
        BufResult(result, buffers)
    }

    async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct Transport {
    pub reader: Reader,
    pub writer: Writer,
}

impl IntoOwnedSplit for Transport {
    type ReadHalf = Reader;
    type WriteHalf = Writer;

    fn into_split(self) -> (Reader, Writer) {
        (self.reader, self.writer)
    }
}
