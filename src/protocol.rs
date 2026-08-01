//! Wire protocol constants and types

pub mod message_type {
    // Control channel messages (0-99)
    pub const CONNECT: u16 = 1;

    // KV domain (100-199)
    pub const KV_BEGIN: u16 = 100;
    pub const KV_COMMIT: u16 = 101;
    pub const KV_ROLLBACK: u16 = 102;
    pub const KV_GET: u16 = 103;
    pub const KV_PUT: u16 = 104;
    pub const KV_DELETE: u16 = 106;
    pub const KV_SUBSCRIBE: u16 = 109;
    pub const KV_UNSUBSCRIBE: u16 = 110;
    pub const KV_NOTIFY: u16 = 111;

    // Queue domain (200-299)
    pub const QUEUE_ENQUEUE: u16 = 200;
    pub const QUEUE_RESERVE: u16 = 202;
    pub const QUEUE_EXTEND: u16 = 203;
    pub const QUEUE_COMPLETE: u16 = 204;
    pub const QUEUE_SUBSCRIBE: u16 = 207;
    pub const QUEUE_UNSUBSCRIBE: u16 = 208;
    pub const QUEUE_NOTIFY: u16 = 209;

    // RPC domain (300-399)
    pub const RPC_SUBSCRIBE: u16 = 300;
    pub const RPC_UNSUBSCRIBE: u16 = 301;
    pub const RPC_REQUEST: u16 = 302;
    pub const RPC_RESPONSE: u16 = 303;

    // Lease domain (400-499)
    pub const LEASE_ACQUIRE: u16 = 400;
    pub const LEASE_RENEW: u16 = 401;
    pub const LEASE_RELEASE: u16 = 402;
    pub const LEASE_QUERY: u16 = 403;
    pub const LEASE_SUBSCRIBE: u16 = 407;
    pub const LEASE_UNSUBSCRIBE: u16 = 408;
    pub const LEASE_NOTIFY: u16 = 409;

    // Notice domain (500-599)
    pub const NOTICE_PUBLISH: u16 = 500;
    pub const NOTICE_SUBSCRIBE: u16 = 501;
    pub const NOTICE_UNSUBSCRIBE: u16 = 502;
    pub const NOTICE_UNSUBSCRIBE_ALL: u16 = 503;
    pub const NOTICE_NOTIFY: u16 = 504;

    // Stream domain (600-699)
    pub const STREAM_BEGIN: u16 = 600;
    pub const STREAM_APPEND: u16 = 601;
    pub const STREAM_COMMIT: u16 = 602;
    pub const STREAM_ROLLBACK: u16 = 603;
    pub const STREAM_READ: u16 = 604;
    pub const STREAM_LAST: u16 = 605;
    pub const STREAM_GET_METADATA: u16 = 606;
    pub const STREAM_SUBSCRIBE: u16 = 607;
    pub const STREAM_UNSUBSCRIBE: u16 = 608;
    pub const STREAM_NOTIFY: u16 = 609; // Server -> Client only

    // Schedule domain (700-799)
    pub const SCHEDULE_CREATE: u16 = 700;
    pub const SCHEDULE_CANCEL: u16 = 701;
    pub const SCHEDULE_LIST: u16 = 702;
    pub const SCHEDULE_SUBSCRIBE: u16 = 703;
    pub const SCHEDULE_UNSUBSCRIBE: u16 = 704;
    pub const SCHEDULE_NOTIFY: u16 = 705; // Server -> Client only
}

/// Transaction mode for KV operations.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum TransactionMode {
    ReadOnly = 0,
    ReadWrite = 1,
}
