use crate::runtime::driver::op::{Completable, CqeResult, Op};
use crate::runtime::CONTEXT;
use std::io;

/// Provide buffers operation for multishot receive
pub struct ProvideBuffers {
    addr: *mut u8,
    len: i32,
    nbufs: u16,
    bgid: u16,
    bid: u16,
}

impl ProvideBuffers {
    pub fn provide_buffers(
        addr: *mut u8,
        len: usize,
        nbufs: usize,
        bgid: u16,
        bid: u16,
    ) -> io::Result<Op<ProvideBuffers>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                ProvideBuffers {
                    addr,
                    len: len as i32,
                    nbufs: nbufs as u16,
                    bgid,
                    bid,
                },
                |provide| {
                    opcode::ProvideBuffers::new(
                        provide.addr,
                        provide.len,
                        provide.nbufs,
                        provide.bgid,
                        provide.bid,
                    )
                    .build()
                },
            )
        })
    }
}

impl Completable for ProvideBuffers {
    type Output = io::Result<()>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        cqe.result.map(|_| ())
    }
}

// SAFETY: ProvideBuffers holds a raw pointer but doesn't actually own the memory
unsafe impl Send for ProvideBuffers {}
