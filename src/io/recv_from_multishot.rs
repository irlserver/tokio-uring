use crate::runtime::driver::op::{Completable, CqeResult, MultiCQEFuture, Op, Updateable};
use crate::runtime::CONTEXT;
use crate::{io::SharedFd, Result};
use socket2::SockAddr;
use std::{
    io,
    net::SocketAddr,
    boxed::Box,
    sync::Arc,
};
use tracing::trace;

/// Callback trait for accessing buffer data by ID
pub trait BufferProvider: Send + Sync {
    fn get_buffer(&self, buffer_id: u16, len: usize) -> &[u8];
}

/// Multishot receive from operation for UDP sockets
/// 
/// This uses io_uring's IORING_RECV_MULTISHOT feature to receive multiple
/// packets with a single SQE submission. Each packet completion returns a
/// buffer from the provided buffer pool.
/// 
/// This implementation batches received packets and yields them in groups
/// for more efficient processing.
pub struct RecvFromMultishot {
    fd: SharedFd,
    buf_group_id: u16,
    socket_addr: Box<SockAddr>,
    msghdr: Box<libc::msghdr>,
    io_slices: Vec<libc::iovec>,
    buffer_provider: Arc<dyn BufferProvider>,
    // Accumulated packets waiting to be yielded
    batch: Vec<RecvFromMultishotResult>,
    batch_size: usize,
    // CRITICAL: These determine the buffer layout for payload offset calculation
    // The kernel RESERVES this much space in the buffer, regardless of actual data
    msg_namelen: usize,
    msg_controllen: usize,
}

impl RecvFromMultishot {
    pub fn recv_from_multishot(
        fd: &SharedFd,
        buf_group_id: u16,
        buffer_provider: Arc<dyn BufferProvider>,
    ) -> io::Result<Op<RecvFromMultishot, MultiCQEFuture>> {
        Self::recv_from_multishot_with_batch_size(fd, buf_group_id, buffer_provider, 32)
    }

    pub fn recv_from_multishot_with_batch_size(
        fd: &SharedFd,
        buf_group_id: u16,
        buffer_provider: Arc<dyn BufferProvider>,
        batch_size: usize,
    ) -> io::Result<Op<RecvFromMultishot, MultiCQEFuture>> {
        use io_uring::{opcode, types};

        // For multishot recvmsg with provided buffers, the kernel writes to the provided buffer:
        //   [RecvMsgOut header (16 bytes)] [name/sockaddr] [control data] [payload]
        //
        // CRITICAL: msg_iovlen must be > 0 and iov_len must indicate max payload size!
        // Even though the provided buffer is used, the kernel checks msg_iov to determine
        // how much payload data to copy. With msg_iovlen=0, no payload is copied.
        //
        // We create a dummy iovec with length = max expected payload (MTU - overhead).
        // The actual buffer pointer doesn't matter since provided buffers are used.
        const MAX_PAYLOAD: usize = 1500; // MTU

        // Leak a small buffer to create a stable pointer for the iovec
        // This is a one-time allocation per multishot operation
        let dummy_buf = Box::leak(Box::new([0u8; 1]));
        let iovec = libc::iovec {
            iov_base: dummy_buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: MAX_PAYLOAD,
        };
        let io_slices = vec![iovec];

        let socket_addr = Box::new(unsafe { SockAddr::try_init(|_, _| Ok(()))?.1 });

        // CRITICAL: msg_namelen and msg_controllen determine the RESERVED space in the buffer!
        // The kernel writes: [RecvMsgOut(16)] [name(msg_namelen)] [control(msg_controllen)] [payload]
        // We must use these values (not the actual data lengths) to find the payload offset.
        let msg_namelen = socket_addr.len() as usize;
        let msg_controllen: usize = 0; // We don't request control data

        let mut msghdr: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        msghdr.msg_iov = io_slices.as_ptr() as *mut libc::iovec;
        msghdr.msg_iovlen = 1; // MUST be > 0 for kernel to copy payload!
        msghdr.msg_name = socket_addr.as_ptr() as *mut libc::c_void;
        msghdr.msg_namelen = msg_namelen as u32;
        msghdr.msg_controllen = msg_controllen as _;

        trace!(
            fd = fd.raw_fd(),
            buf_group_id = buf_group_id,
            batch_size = batch_size,
            msg_namelen = msg_namelen,
            "RecvFromMultishot::submit"
        );

        CONTEXT.with(|x| {
            let result = x.handle().expect("Not in a runtime context").submit_op::<_, MultiCQEFuture, _>(
                RecvFromMultishot {
                    fd: fd.clone(),
                    buf_group_id,
                    socket_addr,
                    msghdr,
                    io_slices,
                    buffer_provider,
                    batch: Vec::with_capacity(batch_size),
                    batch_size,
                    msg_namelen,
                    msg_controllen,
                },
                |recv_from| {
                    let sqe = opcode::RecvMsgMulti::new(
                        types::Fd(recv_from.fd.raw_fd()),
                        recv_from.msghdr.as_mut() as *mut _,
                        recv_from.buf_group_id,
                    )
                    .build();
                    trace!(fd = recv_from.fd.raw_fd(), "RecvMsgMulti SQE created");
                    sqe
                },
            );
            trace!("RecvFromMultishot op submitted successfully");
            result
        })
    }
}

/// Result of a single multishot receive completion
pub struct RecvFromMultishotResult {
    pub bytes_received: usize,
    pub source_addr: SocketAddr,
    pub buffer_id: u16,
    pub payload_offset: usize, // Offset within buffer where payload starts
    pub more: bool, // Whether more packets are expected
}

/// Parse RecvMsgOut header from buffer
///
/// RecvMsgOut structure layout (from io_uring kernel docs):
/// struct io_uring_recvmsg_out {
///     __u32 namelen;      // ACTUAL bytes written for name
///     __u32 controllen;   // ACTUAL bytes written for control
///     __u32 payloadlen;   // ACTUAL bytes written for payload
///     __u32 flags;
/// };
///
/// CRITICAL: The buffer layout uses RESERVED space (from msghdr), not actual data lengths!
/// Buffer layout: [Header(16)] [name(msg_namelen)] [control(msg_controllen)] [payload]
///
/// The actual data is written at the START of each reserved section:
/// - Name data at offset 16, actual length = RecvMsgOut.namelen
/// - Control data at offset 16 + msg_namelen, actual length = RecvMsgOut.controllen
/// - Payload at offset 16 + msg_namelen + msg_controllen, actual length = RecvMsgOut.payloadlen
fn parse_recvmsg_out(buffer: &[u8], msg_namelen: usize, msg_controllen: usize) -> io::Result<(SocketAddr, usize, usize)> {
    // Header is 16 bytes (4 x u32)
    if buffer.len() < 16 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Buffer too small for RecvMsgOut header"));
    }

    // Parse header fields (little-endian) - these are ACTUAL bytes written
    let actual_namelen = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
    let actual_controllen = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;
    let payloadlen = u32::from_le_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]) as usize;
    // flags at offset 12-15 (we don't need them here)

    // Calculate payload offset using RESERVED space (msg_namelen, msg_controllen), not actual data lengths
    let header_size = 16;
    let payload_offset = header_size + msg_namelen + msg_controllen;

    trace!(
        actual_namelen = actual_namelen,
        payloadlen = payloadlen,
        payload_offset = payload_offset,
        "RecvMsgOut parsed"
    );

    // Validate buffer has enough space
    let total_size = payload_offset + payloadlen;
    if buffer.len() < total_size {
        return Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("Buffer too small: expected {}, got {}", total_size, buffer.len())));
    }

    // Parse source address from name data (at offset 16, using actual_namelen)
    let name_data = &buffer[header_size..header_size + actual_namelen];
    let source_addr = if actual_namelen >= std::mem::size_of::<libc::sockaddr_in>() {
        // Parse sockaddr structure using socket2's try_init
        let (_, socket2_addr) = unsafe {
            SockAddr::try_init(|storage_ptr, len_ptr| {
                // Copy the sockaddr data from buffer to storage
                let storage_ptr = storage_ptr as *mut u8;
                std::ptr::copy_nonoverlapping(name_data.as_ptr(), storage_ptr, actual_namelen);
                *len_ptr = actual_namelen as u32;
                Ok(())
            })
        }.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        socket2_addr.as_socket()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid socket address"))?
    } else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Name length too small"));
    };

    Ok((source_addr, payload_offset, payloadlen))
}

impl Completable for RecvFromMultishot {
    type Output = std::result::Result<Vec<RecvFromMultishotResult>, std::io::Error>;

    fn complete(mut self, cqe: CqeResult) -> Self::Output {
        // Process the final CQE (without 'more' flag) and return the accumulated batch
        let flags = cqe.flags;
        let res = cqe.result.map(|v| v as usize);
        
        // If this final CQE has valid data, add it to the batch
        if let Ok(n) = res {
            if let Some(buffer_id) = io_uring::cqueue::buffer_select(flags) {
                let more = io_uring::cqueue::more(flags);
                
                // Parse RecvMsgOut to get source address and payload
                let buffer = self.buffer_provider.get_buffer(buffer_id, n);
                if let Ok((source_addr, payload_offset, payloadlen)) = parse_recvmsg_out(buffer, self.msg_namelen, self.msg_controllen) {
                    self.batch.push(RecvFromMultishotResult {
                        bytes_received: payloadlen,
                        source_addr,
                        buffer_id,
                        payload_offset,
                        more,
                    });
                }
            }
        }
        
        // Return the accumulated batch
        Ok(self.batch)
    }
}

impl Updateable for RecvFromMultishot {
    fn update(&mut self, cqe: CqeResult) {
        // For multishot operations, we accumulate packets in a batch
        // The kernel will continue posting CQEs until the operation is cancelled
        // or an error occurs

        let flags = cqe.flags;
        let res = cqe.result.map(|v| v as usize);

        if let Ok(n) = res {
            // Extract buffer ID from CQE flags
            if let Some(buffer_id) = io_uring::cqueue::buffer_select(flags) {
                // Check if more completions are expected
                let more = io_uring::cqueue::more(flags);

                // Get buffer data and parse RecvMsgOut structure
                let buffer = self.buffer_provider.get_buffer(buffer_id, n);

                match parse_recvmsg_out(buffer, self.msg_namelen, self.msg_controllen) {
                    Ok((source_addr, payload_offset, payloadlen)) => {
                        trace!(
                            %source_addr,
                            payload_offset = payload_offset,
                            payloadlen = payloadlen,
                            buffer_id = buffer_id,
                            more = more,
                            "RecvFromMultishot::update - packet received"
                        );

                        self.batch.push(RecvFromMultishotResult {
                            bytes_received: payloadlen,  // Return payload length, not total buffer size
                            source_addr,
                            buffer_id,
                            payload_offset,
                            more,
                        });
                    }
                    Err(e) => {
                        trace!(error = %e, "RecvFromMultishot::update - Failed to parse RecvMsgOut");
                    }
                }
            } else {
                trace!(flags = flags, "RecvFromMultishot::update - NO buffer_id in flags");
            }
        } else {
            trace!(?res, "RecvFromMultishot::update - ERROR result");
        }
    }
    
    fn should_yield(&self) -> bool {
        // Yield when we have accumulated any packets
        // This ensures buffers are returned promptly even in low-traffic scenarios
        // Previously only yielded on full batch (batch_size), causing buffer exhaustion
        !self.batch.is_empty()
    }
    
    fn yield_result(&mut self) -> Self::Output {
        // Drain the current batch and return it
        // Use mem::replace to swap with a fresh Vec while keeping capacity
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
        Ok(batch)
    }
}
