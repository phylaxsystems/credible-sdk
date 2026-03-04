//! Channel types for sidecar → Aeges communication.

use crate::transaction_observer::ReconstructableTx;

pub type AegesReportSender = flume::Sender<ReconstructableTx>;
pub type AegesReportReceiver = flume::Receiver<ReconstructableTx>;
