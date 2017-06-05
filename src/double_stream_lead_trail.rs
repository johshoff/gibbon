//! This module provides a compressor for `f64`s by looking at the XOR between
//! consecutive values.
//!
//! This is similar to `DoubleStream` except it uses the number of leading
//! bits and meaningful bits to keep a non-shrinking window. The only time
//! The window changes is for explict changes.

pub use stream::{Writer, Reader};
use std::cmp::min;
use bit_repr::to_bits;

pub enum DoubleStreamStateLeadTrail {
    Initial,
    Following {
        value: u64,
        leading_zeros: u8,
        meaningful_count: u8,
    }
}

pub struct DoubleStreamLeadTrail {
    state: DoubleStreamStateLeadTrail
}

impl DoubleStreamLeadTrail {
    // TODO: This is in large part a verbatim copy of `impl DoubleStream` with
    // a few changes. Once a clear winner has been crowned one of the
    // implementations can be removed. If no such winner is found, some code
    // could probably be extracted.
    pub fn new() -> Self {
        DoubleStreamLeadTrail {
            state: DoubleStreamStateLeadTrail::Initial
        }
    }

    pub fn push(&mut self, number: f64, writer: &mut Writer) {
        let number_as_bits = to_bits(number);

        self.state = match self.state {
            DoubleStreamStateLeadTrail::Initial => {
                writer.write(number_as_bits, 64);
                DoubleStreamStateLeadTrail::Following {
                  value: number_as_bits,
                  leading_zeros: 64, // force window to be redefined
                  meaningful_count: 0
                }
            },
            DoubleStreamStateLeadTrail::Following { value: previous, leading_zeros: prev_lz, meaningful_count: prev_meaningful } => {
                let xored = previous ^ number_as_bits;
                match xored {
                    0 => {
                        writer.write(0, 1);

                        DoubleStreamStateLeadTrail::Following {
                            value: number_as_bits,
                            // Made a choice here to keep the current window. Seems like a good
                            leading_zeros: prev_lz,
                            meaningful_count: prev_meaningful
                        }
                    },
                    _ => {
                        let lz = min(xored.leading_zeros() as u8, 31); // [LEADING31]
                        let tz = xored.trailing_zeros() as u8;
                        assert!(lz < 32); // otherwise can't be stored in 5 bits
                        // we must assume at least one meaningful bit!

                        let prev_tz = 64 - prev_lz - prev_meaningful;

                        if lz >= prev_lz && tz >= prev_tz {
                            // fit into the previous window
                            let meaningful_bits = xored >> prev_tz;
                            let meaningful_bit_count = 64 - prev_tz - prev_lz;

                            writer.write(0b10, 2);
                            writer.write(meaningful_bits, meaningful_bit_count as u8);

                            // keep window size
                            DoubleStreamStateLeadTrail::Following {
                                value: number_as_bits,
                                leading_zeros: prev_lz,
                                meaningful_count: prev_meaningful
                            }
                        } else {
                            // create a new window with leading and trailing zeros
                            let meaningful_bits = xored >> tz;

                            // if tz and lz are 0, meaningful bits is 64, which can't be stored in 6 bits, so we
                            // must assume at least one significant bit, which we safely can since the xored
                            // value is not 0
                            let meaningful_bit_count = 64 - tz - lz;

                            assert!(meaningful_bit_count <= 64);
                            writer.write(0b11, 2);
                            writer.write(lz as u64, 5);
                            writer.write((meaningful_bit_count - 1) as u64, 6); // [MEANING64]
                            writer.write(meaningful_bits, meaningful_bit_count as u8);

                            DoubleStreamStateLeadTrail::Following {
                                value: number_as_bits,
                                leading_zeros: lz,
                                meaningful_count: 64 - tz
                            }
                        }
                    }
                }
            }
        };
    }
}
