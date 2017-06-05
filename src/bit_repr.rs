//! This module will be removed once `float_bits_conv` is stable. At that point
//! `to_bits(x)` should be replaced by `x.to_bits()` and `from_bits(x)` by
//! `x.from_bits()`.

use std::mem;

pub fn to_bits(i: f64) -> u64 {
    unsafe { mem::transmute(i) }
}

pub fn from_bits(i: u64) -> f64 {
    unsafe { mem::transmute(i) }
}


