//! Lossy numeric type conversions for metrics reporting.

/// Convert a `usize` to `f64`, losing precision for values above 2^53.
#[must_use]
pub fn lossy_usize_to_f64(value: usize) -> f64 {
    u64_to_f64_lossy(u64::try_from(value).unwrap_or(u64::MAX))
}

/// Convert a `u64` to `f64`, losing precision for values above 2^53.
#[must_use]
pub fn lossy_u64_to_f64(value: u64) -> f64 {
    u64_to_f64_lossy(value)
}

/// Split a `u64` into high and low `u32` halves and recombine as `f64`.
#[must_use]
pub fn u64_to_f64_lossy(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(0);
    let low = u32::try_from(value & 0xFFFF_FFFF).unwrap_or(0);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}
