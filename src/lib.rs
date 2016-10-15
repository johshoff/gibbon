use std::mem;
use std::u64;
use std::cmp::min;
use std::iter::Iterator;
pub mod vec_stream;
pub mod stream;
pub use stream::*;

pub enum IntStreamState {
    Initial {
        header_time: u64 // aligned to a two hour window
    },
    Following {
        value: u64,
        delta: i64,
    },
}

pub struct IntStream<W: Writer> {
    state: IntStreamState,
    writer: W,
}

impl<W: Writer> IntStream<W> {
    pub fn new(writer: W, header_time: u64) -> Self {
        IntStream {
            state: IntStreamState::Initial { header_time: header_time },
            writer: writer,
        }
    }

    pub fn push(&mut self, number: u64) {
        let delta = match self.state {
            IntStreamState::Initial { header_time } => {
                assert!(number >= header_time); // header time should be rounded down
                let delta = number - header_time;
                assert!(delta <= (1 << 14)); // enough to store more than four hours in seconds
                self.writer.write(number, 14);

                delta as i64
            },
            IntStreamState::Following { value: prev_value, delta: prev_delta } => {
                let delta = (number - prev_value) as i64;
                let delta_of_deltas = delta - prev_delta;
                let delta_of_deltas_bits : u64 = unsafe { mem::transmute(delta_of_deltas) };

                // will only work assuming two's compliment architecture
                if delta_of_deltas == 0 {
                    self.writer.write(0, 1);
                } else if delta_of_deltas >= -63 && delta_of_deltas <= 64 {
                    self.writer.write(0b10, 2);
                    self.writer.write(delta_of_deltas_bits & ((1 << 7) - 1), 7);
                } else if delta_of_deltas >= -255 && delta_of_deltas <= 256 {
                    self.writer.write(0b110, 3);
                    self.writer.write(delta_of_deltas_bits & ((1 << 9) - 1), 9);
                } else if delta_of_deltas >= -2047 && delta_of_deltas <= 2048 {
                    self.writer.write(0b1110, 4);
                    self.writer.write(delta_of_deltas_bits & ((1 << 12) - 1), 12);
                } else {
                    self.writer.write(0b1111, 4);
                    self.writer.write(delta_of_deltas_bits & ((1 << 32) - 1), 32);
                }

                delta
            }
        };

        self.state = IntStreamState::Following {
            value: number,
            delta: delta
        };
    }
}



pub struct IntStreamIterator<R> where R: Reader {
    reader: R,
    state: IntStreamState,
}

impl<R> IntStreamIterator<R> where R: Reader {
    pub fn new(reader: R, header_time: u64) -> Self {
        IntStreamIterator {
            reader: reader,
            state: IntStreamState::Initial { header_time: header_time },
        }
    }
}

impl<R> Iterator for IntStreamIterator<R> where R: Reader {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let values = match self.state {
            IntStreamState::Initial { header_time } => {
                self.reader.read(14).and_then(|delta| Some((header_time + delta, delta as i64)))
            }
            IntStreamState::Following { value, delta } => {
                match self.reader.read(1) {
                    Some(0) => Some((value.wrapping_add(delta as u64), delta)),
                    Some(1) => {
                        // unwrapping reads from now on, on the assumption that the stream is
                        // well-formed

                        // TODO: signed delta_of_deltas

                        let num_bits = if self.reader.read(1).unwrap() == 0 { // 10
                            7
                        } else if self.reader.read(1).unwrap() == 0 { // 110
                            9
                        } else if self.reader.read(1).unwrap() == 0 { // 1110
                            12
                        } else { // 1111
                            32
                        };

                        let mut delta_of_deltas = self.reader.read(num_bits).unwrap();
                        let msb = 1 << (num_bits - 1); // value of most significant bit
                        if delta_of_deltas & msb != 0 {
                            // propagate two's compliment sign to all 64 bits
                            delta_of_deltas |= !(msb - 1);
                        }

                        let new_delta = delta + (delta_of_deltas as i64);
                        let new_value = value.wrapping_add(new_delta as u64);
                        Some((new_value, new_delta))
                    }
                    None => None,
                    _ => panic!("Binary read should not be able to return anything but 0 or 1")
                }
            }
        };

        if let Some((value, delta)) = values {
            self.state = IntStreamState::Following { value: value, delta: delta };
            Some(value)
        } else {
            None
        }
    }
}


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

    #[test]
    fn all_zeros_int() {
        let mut c = IntStream::new(StringWriter::new(), 0);
        c.push(0); assert_eq!(c.writer.string, "00000000000000");
        c.push(0); assert_eq!(c.writer.string, "000000000000000");
        c.push(0); assert_eq!(c.writer.string, "0000000000000000");
        c.push(0); assert_eq!(c.writer.string, "00000000000000000");
        c.push(0); assert_eq!(c.writer.string, "000000000000000000");

        let mut r = IntStreamIterator::new(StringReader::new(c.writer.string), 0); // TODO: change to DoubleStreamIterator and watch it... PASS?!
        assert_eq!(r.next().unwrap(), 0);
        assert_eq!(r.next().unwrap(), 0);
        assert_eq!(r.next().unwrap(), 0);
        assert_eq!(r.next().unwrap(), 0);
        assert_eq!(r.next().unwrap(), 0);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn int_less_than_64() {
        let mut c = IntStream::new(StringWriter::new(), 0);
        c.push(1); assert_eq!(c.writer.string, "00000000000001");                       // delta 1
        c.push(2); assert_eq!(c.writer.string, "000000000000010");                      // delta 1, dod = 0
        c.push(3); assert_eq!(c.writer.string, "0000000000000100");                     // delta 1, dod = 0
        c.push(4); assert_eq!(c.writer.string, "00000000000001000");                    // delta 1, dod = 0
        c.push(4); assert_eq!(c.writer.string, "00000000000001000101111111");           // delta 0, dod = -1
        c.push(4); assert_eq!(c.writer.string, "000000000000010001011111110");          // delta 0, dod = 0
        c.push(6); assert_eq!(c.writer.string, "000000000000010001011111110100000010"); // delta 2, dod = 2

        let mut r = IntStreamIterator::new(StringReader::new(c.writer.string), 0);
        assert_eq!(r.next().unwrap(), 1);
        assert_eq!(r.next().unwrap(), 2);
        assert_eq!(r.next().unwrap(), 3);
        assert_eq!(r.next().unwrap(), 4);
        assert_eq!(r.next().unwrap(), 4);
        assert_eq!(r.next().unwrap(), 4);
        assert_eq!(r.next().unwrap(), 6);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn int_all_steps() {
        let mut c = IntStream::new(StringWriter::new(), 0);
        c.push(    1); assert_eq!(c.writer.string, "00000000000001");                                                                          // delta     1
        c.push(   51); assert_eq!(c.writer.string, "00000000000001100110001");                                                                 // delta    50, dod = 49
        c.push(  251); assert_eq!(c.writer.string, "00000000000001100110001110010010110");                                                     // delta   200, dod = 150
        c.push( 1251); assert_eq!(c.writer.string, "000000000000011001100011100100101101110001100100000");                                     // delta  1000, dod = 800
        c.push(11251); assert_eq!(c.writer.string, "000000000000011001100011100100101101110001100100000111100000000000000000010001100101000"); // delta 10000, dod = 9000

        let mut r = IntStreamIterator::new(StringReader::new(c.writer.string), 0);
        assert_eq!(r.next().unwrap(),     1);
        assert_eq!(r.next().unwrap(),    51);
        assert_eq!(r.next().unwrap(),   251);
        assert_eq!(r.next().unwrap(),  1251);
        assert_eq!(r.next().unwrap(), 11251);
        assert_eq!(r.next(), None);
    }

    #[test]
    fn fuzzer_vec_int () {
        // throw some random values at it and see if they decode correctly
        let header_time = 0;
        let mut c = IntStream::new(vec_stream::VecWriter::new(), header_time);
        let mut numbers = Vec::new();

        for i in header_time..1_000 {
            c.push(i);
            numbers.push(i);
        }

        let r = IntStreamIterator::new(vec_stream::VecReader::new(c.writer.bit_vector, c.writer.used_bits_last_elm), header_time);

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }
}
