//! A double stream compresses and decompresses `f64` numbers by looking at the
//! XOR between consecutive values.
//!
//! Per [XORORLEADING] it's unclear to me if the xor-value itself should be
//! stored and used or if the number of leading and meaningful bits (or
//! trailing bits) should be stored and used. This is an implementation of
//! the former and will lead to a shrinking window size as more leading or
//! trailing zeroes are available. See `DoubleStreamLeadTrail` for an
//! implementation of the latter.

pub use stream::{Writer, Reader};
use std::cmp::min;

pub enum DoubleStreamState {
    Initial,
    Following {
        value: u64,
        xor: u64,
    }
}

pub struct DoubleStreamWriter {
    state: DoubleStreamState
}

impl DoubleStreamWriter {
    pub fn new() -> Self {
        DoubleStreamWriter {
            state: DoubleStreamState::Initial
        }
    }

    pub fn push(&mut self, number: f64, writer: &mut Writer) {
        let number_as_bits = number.to_bits();

        self.state = match self.state {
            DoubleStreamState::Initial => {
                writer.write(number_as_bits, 64);
                DoubleStreamState::Following { value: number_as_bits, xor: number_as_bits }
            },
            DoubleStreamState::Following { value: previous, xor: prev_xor } => {
                let xored = previous ^ number_as_bits;
                match xored {
                    0 => writer.write(0, 1),
                    _ => {
                        let lz = min(xored.leading_zeros() as u64, 31); // [LEADING31]
                        let tz = xored.trailing_zeros() as u64;
                        assert!(lz < 32); // otherwise can't be stored in 5 bits
                        // we must assume at least one meaningful bit!

                        // [CLARIFY] should be prev_xor or prev_value below?
                        let prev_lz = prev_xor.leading_zeros() as u64;
                        let prev_tz = if prev_lz == 64 { 0 } else { prev_xor.trailing_zeros() as u64 }; // [OPTIMALIZATION] don't need to always calculate this one

                        if lz >= prev_lz && tz >= prev_tz {
                            // fit into the previous window
                            let meaningful_bits = xored >> prev_tz;
                            let meaningful_bit_count = 64 - prev_tz - prev_lz;

                            writer.write(0b10, 2);
                            writer.write(meaningful_bits, meaningful_bit_count as u8);
                        } else {
                            // create a new window with leading and trailing zeros
                            let meaningful_bits = xored >> tz;

                            // if tz and lz are 0, meaningful bits is 64, which can't be stored in 6 bits, so we
                            // must assume at least one significant bit, which we safely can since the xored
                            // value is not 0
                            let meaningful_bit_count = 64 - tz - lz;

                            assert!(meaningful_bit_count <= 64);
                            writer.write(0b11, 2);
                            writer.write(lz, 5);
                            writer.write(meaningful_bit_count - 1, 6); // [MEANING64]
                            writer.write(meaningful_bits, meaningful_bit_count as u8);
                        }
                    }
                }
                DoubleStreamState::Following { value: number_as_bits, xor: xored }
            }
        };
    }
}

pub struct DoubleStreamParser {
    state: DoubleStreamState,
}

impl DoubleStreamParser {
    pub fn new() -> Self {
        DoubleStreamParser {
            state: DoubleStreamState::Initial
        }
    }

    pub fn next(&mut self, reader: &mut Reader) -> Option<f64> {
        let values = match self.state {
            DoubleStreamState::Initial => {
                reader.read(64).and_then(|x| Some((x, x)))
            }
            DoubleStreamState::Following { value, xor } => {
                match reader.read(1) {
                    Some(0) => Some((value, xor)),
                    Some(1) => {
                        // unwrapping reads from now on, on the assumption that the stream is
                        // well-formed
                        match reader.read(1).unwrap() {
                            0 => { // reuse window
                                let prev_lz = xor.leading_zeros() as u64;
                                let prev_tz = if prev_lz == 64 { 0 } else { xor.trailing_zeros() as u64 };
                                let meaningful_bit_count = 64 - prev_tz - prev_lz;

                                let new_xor = reader.read(meaningful_bit_count as u8).unwrap() << prev_tz;
                                let new_value = value ^ new_xor;
                                Some((new_value, new_xor))
                            },
                            1 => { // new window
                                let lz = reader.read(5).unwrap();
                                let meaningful_bit_count = reader.read(6).unwrap() + 1;
                                let tz = 64 - meaningful_bit_count - lz;

                                let new_xor = reader.read(meaningful_bit_count as u8).unwrap() << tz;
                                let new_value = value ^ new_xor;
                                Some((new_value, new_xor))
                            },
                            _ => panic!("Binary read should not be able to return anything but 0 or 1")
                        }
                    }
                    None => None,
                    _ => panic!("Binary read should not be able to return anything but 0 or 1")
                }
            }
        };

        if let Some((value, xor)) = values {
            self.state = DoubleStreamState::Following { value: value, xor: xor };
            Some(f64::from_bits(value))
        } else {
            None
        }
    }
}

pub struct DoubleStreamIterator<R: Reader> {
    parser: DoubleStreamParser,
    reader: R,
}

impl<R> DoubleStreamIterator<R> where R: Reader{
    pub fn new(reader: R) -> Self {
        DoubleStreamIterator {
            parser: DoubleStreamParser::new(),
            reader: reader,
        }
    }
}

impl<R> Iterator for DoubleStreamIterator<R> where R: Reader {
    type Item = f64;

    fn next(&mut self) -> Option<f64> {
        self.parser.next(&mut self.reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bit_string_stream::*;
    use vec_stream::{VecWriter, VecReader};

    #[test]
    fn all_zeros() {
        // using XOR == 0 rule (0)
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        c.push(0f64, &mut w); assert_eq!(w.string, "0000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64, &mut w); assert_eq!(w.string, "00000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64, &mut w); assert_eq!(w.string, "000000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64, &mut w); assert_eq!(w.string, "0000000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64, &mut w); assert_eq!(w.string, "00000000000000000000000000000000000000000000000000000000000000000000");

        let mut r = DoubleStreamIterator::new(StringReader::new(w.string));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn new_window() {
        // using "new window" rule (11)
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        c.push(0f64, &mut w); assert_eq!(w.string, "0000000000000000000000000000000000000000000000000000000000000000");
        // one: 0011111111110000000000000000000000000000000000000000000000000000
        // L = leading zeros, #M = number of meaningful bits, meanfbits = the meaningful bits themselves -->       11[ L ][#M-1][meanbits]
        c.push(1f64, &mut w); assert_eq!(w.string, "000000000000000000000000000000000000000000000000000000000000000011000100010011111111111");

        let mut r = DoubleStreamIterator::new(StringReader::new(w.string));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(1f64));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn reuse_window() {
        // using "old window" rule (10)
        // eleven: 0100000000100110000000000000000000000000000000000000000000000000
        // ten:    0100000000100100000000000000000000000000000000000000000000000000
        // xor:    0000000000000010000000000000000000000000000000000000000000000000
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        c.push(11f64, &mut w); assert_eq!(w.string, "0100000000100110000000000000000000000000000000000000000000000000");
        //                               window start ^            ^ window end
        //                                            [previous wnd]   ----------------------------------------------->
        // L = leading zeros, #M = number of meaningful bits, meanfbits = the meaningful bits themselves -->         10[previous wnd]
        c.push(10f64, &mut w); assert_eq!(w.string, "01000000001001100000000000000000000000000000000000000000000000001000000000000001");

        let mut r = DoubleStreamIterator::new(StringReader::new(w.string));
        assert_eq!(r.next(), Some(11f64));
        assert_eq!(r.next(), Some(10f64));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn all_significant_bits () {
        // what happens if we need to create a new window using all the signficant bits
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        let all_significant = f64::from_bits(0b1000000000000000000000000000000000000000000000000000000000000001u64); // a valid number = -0.5e-323

        // should not crash -- reflecting a change I did not present in the paper (but probably
        // assumed?), namely to store signficant bits - 1 in the significant bit field
        c.push(11f64, &mut w);           // 0100000000100110000000000000000000000000000000000000000000000000
        c.push(all_significant, &mut w); // 1000000000000000000000000000000000000000000000000000000000000001

        let mut r = DoubleStreamIterator::new(StringReader::new(w.string));
        assert_eq!(r.next(), Some(11f64));
        assert_eq!(r.next(), Some(all_significant));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn many_leading_decimals () {
        // using new window rule (11)
        // what happens if we need to create a new window where there are more than 32 leading zeros
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        let last_significant = f64::from_bits(0b0000000000000000000000000000000000000000000000000000000000000001u64); // a valid number = 0.5e-323

        // should not crash -- reflecting a change I did not present in the paper (but probably
        // assumed?), namely to store signficant bits - 1 in the significant bit field
        c.push(0f64, &mut w);             // 0000000000000000000000000000000000000000000000000000000000000000
        c.push(last_significant, &mut w); // 0000000000000000000000000000000000000000000000000000000000000001
        // xor                       0000000000000000000000000000000000000000000000000000000000000001
        //                                                                                           11[ L ][#M-1][meanbits                       ]
        assert_eq!(w.string, "00000000000000000000000000000000000000000000000000000000000000001111111100000000000000000000000000000000000001");

        let mut r = DoubleStreamIterator::new(StringReader::new(w.string));
        assert_eq!(r.next(), Some(0f64));
        assert_eq!(r.next(), Some(last_significant));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn fuzzer () {
        // throw some random values at it and see if they decode correctly
        let mut w = StringWriter::new();
        let mut c = DoubleStreamWriter::new();
        let mut numbers = Vec::new();

        for i in 0..1_000 {
            let i = i as f64;
            c.push(i, &mut w);
            numbers.push(i);
        }

        let r = DoubleStreamIterator::new(StringReader::new(w.string));

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }

    #[test]
    fn fuzzer_vec () {
        // throw some random values at it and see if they decode correctly
        let mut w = VecWriter::new();
        let mut c = DoubleStreamWriter::new();
        let mut numbers = Vec::new();

        for i in 0..1_000 {
            let i = i as f64;
            c.push(i, &mut w);
            numbers.push(i);
        }

        let r = DoubleStreamIterator::new(VecReader::new(&w.bit_vector, w.used_bits_last_elm));

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }

    #[test]
    fn read_aligned_64() {
        // This test case triggers a read of 64 bits exactly aligned to the word boundry.
        let case = vec![-75.01536474599993, -75.00911189799993, 114.37647545700004];

        let mut writer = VecWriter::new();
        let mut stream = DoubleStreamWriter::new();

        for value in case.iter().copied() {
            stream.push(value, &mut writer);
        }

        let VecWriter {
            bit_vector,
            used_bits_last_elm,
        } = writer;

        let reader = DoubleStreamIterator::new(VecReader::new(&bit_vector, used_bits_last_elm));

        let read: Vec<f64> = reader.collect();
        assert_eq!(&read[..], &case[..]);
    }
}
