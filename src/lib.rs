use std::mem;
use std::u64;
use std::cmp::min;
use std::iter::Iterator;
pub mod vec_stream;
pub mod stream;
pub use stream::*;

pub enum DoubleStreamState {
    Initial,
    Following {
        value: u64,
        xor: u64, // [XORORLEADING] replace with leading_zeros: u8 and meaningful_count: u8?
    }
}

pub struct DoubleStream<W: Writer> {
    state: DoubleStreamState,
    writer: W,
}

pub fn as_bits(i: f64) -> u64 {
    unsafe { mem::transmute(i) }
}

impl<W: Writer> DoubleStream<W> {
    pub fn new(writer: W) -> Self {
        DoubleStream {
            state: DoubleStreamState::Initial,
            writer: writer,
        }
    }

    pub fn push(&mut self, number: f64) {
        let number_as_bits = as_bits(number);

        self.state = match self.state {
            DoubleStreamState::Initial => {
                self.writer.write(number_as_bits, 64);
                DoubleStreamState::Following { value: number_as_bits, xor: number_as_bits }
            },
            DoubleStreamState::Following { value: previous, xor: prev_xor } => {
                let xored = previous ^ number_as_bits;
                match xored {
                    0 => self.writer.write(0, 1),
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

                            self.writer.write(0b10, 2);
                            self.writer.write(meaningful_bits, meaningful_bit_count as u8);
                        } else {
                            // create a new window with leading and trailing zeros
                            let meaningful_bits = xored >> tz;

                            // if tz and lz are 0, meaningful bits is 64, which can't be stored in 6 bits, so we
                            // must assume at least one significant bit, which we safely can since the xored
                            // value is not 0
                            let meaningful_bit_count = 64 - tz - lz;

                            assert!(meaningful_bit_count <= 64);
                            self.writer.write(0b11, 2);
                            self.writer.write(lz, 5);
                            self.writer.write(meaningful_bit_count - 1, 6); // [MEANING64]
                            self.writer.write(meaningful_bits, meaningful_bit_count as u8);
                        }
                    }
                }
                DoubleStreamState::Following { value: number_as_bits, xor: xored }
            }
        };
    }
}

pub struct DoubleStreamIterator<R> where R: Reader {
    reader: R,
    state: DoubleStreamState,
}

impl<R> DoubleStreamIterator<R> where R: Reader {
    pub fn new(reader: R) -> Self {
        DoubleStreamIterator {
            reader: reader,
            state: DoubleStreamState::Initial,
        }
    }
}

impl<R> Iterator for DoubleStreamIterator<R> where R: Reader {
    type Item = f64;

    fn next(&mut self) -> Option<f64> {
        let values = match self.state {
            DoubleStreamState::Initial => {
                self.reader.read(64).and_then(|x| Some((x, x)))
            }
            DoubleStreamState::Following { value, xor } => {
                match self.reader.read(1) {
                    Some(0) => Some((value, xor)),
                    Some(1) => {
                        // unwrapping reads from now on, on the assumption that the stream is
                        // well-formed
                        match self.reader.read(1).unwrap() {
                            0 => { // reuse window
                                let prev_lz = xor.leading_zeros() as u64;
                                let prev_tz = if prev_lz == 64 { 0 } else { xor.trailing_zeros() as u64 };
                                let meaningful_bit_count = 64 - prev_tz - prev_lz;

                                let new_xor = self.reader.read(meaningful_bit_count as u8).unwrap() << prev_tz;
                                let new_value = value ^ new_xor;
                                Some((new_value, new_xor))
                            },
                            1 => { // new window
                                let lz = self.reader.read(5).unwrap();
                                let meaningful_bit_count = self.reader.read(6).unwrap() + 1;
                                let tz = 64 - meaningful_bit_count - lz;

                                let new_xor = self.reader.read(meaningful_bit_count as u8).unwrap() << tz;
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
            Some(unsafe { mem::transmute(value) })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;
    use std::u64;

    struct StringWriter {
        string: String
    }

    struct StringReader {
        string: String,
        position: usize,
    }

    impl StringWriter {
        fn new() -> Self {
            StringWriter {
                string: String::new()
            }
        }
    }

    impl StringReader {
        fn new(string: String) -> Self {
            StringReader {
                string: string,
                position: 0,
            }
        }
    }

    impl Writer for StringWriter {
        fn write(&mut self, bits: u64, count: u8) {
            let formatted = &format!("{:0width$b}", bits, width = count as usize);
            assert_eq!(formatted.len(), count as usize);
            self.string.push_str(formatted);
        }
    }

    impl Reader for StringReader {
        fn read(&mut self, count: u8) -> Option<u64> {
            let start_position = self.position;
            let end_position = start_position + count as usize;

            if end_position <= self.string.len() {
                self.position = end_position;

                let bits_as_string = &self.string[start_position..end_position];
                Some(u64::from_str_radix(bits_as_string, 2).unwrap())
            } else {
                None
            }
        }
    }

    #[test]
    fn all_zeros() {
        // using XOR == 0 rule (0)
        let mut c = DoubleStream::new(StringWriter::new());
        c.push(0f64); assert_eq!(c.writer.string, "0000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64); assert_eq!(c.writer.string, "00000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64); assert_eq!(c.writer.string, "000000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64); assert_eq!(c.writer.string, "0000000000000000000000000000000000000000000000000000000000000000000");
        c.push(0f64); assert_eq!(c.writer.string, "00000000000000000000000000000000000000000000000000000000000000000000");

        let mut r = DoubleStreamIterator::new(StringReader::new(c.writer.string));
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn new_window() {
        // using "new window" rule (11)
        let mut c = DoubleStream::new(StringWriter::new());
        c.push(0f64); assert_eq!(c.writer.string, "0000000000000000000000000000000000000000000000000000000000000000");
        // one: 0011111111110000000000000000000000000000000000000000000000000000
        // L = leading zeros, #M = number of meaningful bits, meanfbits = the meaningful bits themselves -->       11[ L ][#M-1][meanbits]
        c.push(1f64); assert_eq!(c.writer.string, "000000000000000000000000000000000000000000000000000000000000000011000100010011111111111");

        let mut r = DoubleStreamIterator::new(StringReader::new(c.writer.string));
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), 1f64);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn reuse_window() {
        // using "old window" rule (10)
        // eleven: 0100000000100110000000000000000000000000000000000000000000000000
        // ten:    0100000000100100000000000000000000000000000000000000000000000000
        // xor:    0000000000000010000000000000000000000000000000000000000000000000
        let mut c = DoubleStream::new(StringWriter::new());
        c.push(11f64); assert_eq!(c.writer.string, "0100000000100110000000000000000000000000000000000000000000000000");
        //                               window start ^            ^ window end
        //                                            [previous wnd]   ----------------------------------------------->
        // L = leading zeros, #M = number of meaningful bits, meanfbits = the meaningful bits themselves -->         10[previous wnd]
        c.push(10f64); assert_eq!(c.writer.string, "01000000001001100000000000000000000000000000000000000000000000001000000000000001");

        let mut r = DoubleStreamIterator::new(StringReader::new(c.writer.string));
        assert_eq!(r.next().unwrap(), 11f64);
        assert_eq!(r.next().unwrap(), 10f64);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn all_significant_bits () {
        // what happens if we need to create a new window using all the signficant bits
        let mut c = DoubleStream::new(StringWriter::new());
        let all_significant = unsafe { mem::transmute::<u64, f64>(0b1000000000000000000000000000000000000000000000000000000000000001u64) }; // a valid number = -0.5e-323

        // should not crash -- reflecting a change I did not present in the paper (but probably
        // assumed?), namely to store signficant bits - 1 in the significant bit field
        c.push(11f64);           // 0100000000100110000000000000000000000000000000000000000000000000
        c.push(all_significant); // 1000000000000000000000000000000000000000000000000000000000000001

        let mut r = DoubleStreamIterator::new(StringReader::new(c.writer.string));
        assert_eq!(r.next().unwrap(), 11f64);
        assert_eq!(r.next().unwrap(), all_significant);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn many_leading_decimals () {
        // using new window rule (11)
        // what happens if we need to create a new window where there are more than 32 leading zeros
        let mut c = DoubleStream::new(StringWriter::new());
        let last_significant = unsafe { mem::transmute::<u64, f64>(0b0000000000000000000000000000000000000000000000000000000000000001u64) }; // a valid number = 0.5e-323

        // should not crash -- reflecting a change I did not present in the paper (but probably
        // assumed?), namely to store signficant bits - 1 in the significant bit field
        c.push(0f64);             // 0000000000000000000000000000000000000000000000000000000000000000
        c.push(last_significant); // 0000000000000000000000000000000000000000000000000000000000000001
        // xor                       0000000000000000000000000000000000000000000000000000000000000001
        //                                                                                           11[ L ][#M-1][meanbits                       ]
        assert_eq!(c.writer.string, "00000000000000000000000000000000000000000000000000000000000000001111111100000000000000000000000000000000000001");

        let mut r = DoubleStreamIterator::new(StringReader::new(c.writer.string));
        assert_eq!(r.next().unwrap(), 0f64);
        assert_eq!(r.next().unwrap(), last_significant);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn fuzzer () {
        // throw some random values at it and see if they decode correctly
        let mut c = DoubleStream::new(StringWriter::new());
        let mut numbers = Vec::new();

        for i in 0..1_000 {
            let i = i as f64;
            c.push(i);
            numbers.push(i);
        }

        let r = DoubleStreamIterator::new(StringReader::new(c.writer.string));

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }

    #[test]
    fn fuzzer_vec () {
        // throw some random values at it and see if they decode correctly
        let mut c = DoubleStream::new(vec_stream::VecWriter::new());
        let mut numbers = Vec::new();

        for i in 0..1_000 {
            let i = i as f64;
            c.push(i);
            numbers.push(i);
        }

        let r = DoubleStreamIterator::new(vec_stream::VecReader::new(c.writer.bit_vector, c.writer.used_bits_last_elm));

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }
}
