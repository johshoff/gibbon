//! This is a compound stream consisting of a timestamp followed by a double.
//! This is how Gorilla compresses streams.

use timestamp_stream::*;
use double_stream::*;

pub struct TimeAndValueStream {
    timestamps: TimestampStreamWriter,
    values: DoubleStreamWriter,
}

impl TimeAndValueStream {
    pub fn new(header_time: u64) -> Self {
        TimeAndValueStream {
            timestamps: TimestampStreamWriter::new(header_time),
            values: DoubleStreamWriter::new(),
        }
    }

    pub fn push(&mut self, timestamp: u64, number: f64, writer: &mut Writer) {
        self.timestamps.push(timestamp, writer);
        self.values.push(number, writer);
    }
}

pub struct TimeAndValueIterator<R: Reader> {
    timestamp_parser: TimestampStreamParser,
    value_parser: DoubleStreamParser,
    reader: R,
}

impl<R> TimeAndValueIterator<R> where R: Reader{
    pub fn new(reader: R, header_time: u64) -> Self {
        TimeAndValueIterator {
            timestamp_parser: TimestampStreamParser::new(header_time),
            value_parser: DoubleStreamParser::new(),
            reader: reader,
        }
    }
}

impl<R> Iterator for TimeAndValueIterator<R> where R: Reader {
    type Item = (u64, f64);

    fn next(&mut self) -> Option<(u64, f64)> {
        // unwrap second result with the assumption that the stream is welformed and we don't get
        // access partial access to it
        self.timestamp_parser.next(&mut self.reader)
            .and_then(|timestamp| Some((timestamp, self.value_parser.next(&mut self.reader).unwrap())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vec_stream::{VecWriter, VecReader};
    use bit_string_stream::*;

    #[test]
    fn all_zeros_int() {
        let mut w = StringWriter::new();
        let mut c = TimestampStreamWriter::new(0);
        c.push(0, &mut w); assert_eq!(w.string, "00000000000000");
        c.push(0, &mut w); assert_eq!(w.string, "000000000000000");
        c.push(0, &mut w); assert_eq!(w.string, "0000000000000000");
        c.push(0, &mut w); assert_eq!(w.string, "00000000000000000");
        c.push(0, &mut w); assert_eq!(w.string, "000000000000000000");

        let mut r = TimestampStreamIterator::new(StringReader::new(w.string), 0);
        assert_eq!(r.next(), Some(0));
        assert_eq!(r.next(), Some(0));
        assert_eq!(r.next(), Some(0));
        assert_eq!(r.next(), Some(0));
        assert_eq!(r.next(), Some(0));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn int_less_than_64() {
        let mut w = StringWriter::new();
        let mut c = TimestampStreamWriter::new(0);
        c.push(1, &mut w); assert_eq!(w.string, "00000000000001");                       // delta 1
        c.push(2, &mut w); assert_eq!(w.string, "000000000000010");                      // delta 1, dod = 0
        c.push(3, &mut w); assert_eq!(w.string, "0000000000000100");                     // delta 1, dod = 0
        c.push(4, &mut w); assert_eq!(w.string, "00000000000001000");                    // delta 1, dod = 0
        c.push(4, &mut w); assert_eq!(w.string, "00000000000001000100111110");           // delta 0, dod = -1
        c.push(4, &mut w); assert_eq!(w.string, "000000000000010001001111100");          // delta 0, dod = 0
        c.push(6, &mut w); assert_eq!(w.string, "000000000000010001001111100101000001"); // delta 2, dod = 2

        let mut r = TimestampStreamIterator::new(StringReader::new(w.string), 0);
        assert_eq!(r.next(), Some(1));
        assert_eq!(r.next(), Some(2));
        assert_eq!(r.next(), Some(3));
        assert_eq!(r.next(), Some(4));
        assert_eq!(r.next(), Some(4));
        assert_eq!(r.next(), Some(4));
        assert_eq!(r.next(), Some(6));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn int_all_steps() {
        let mut w = StringWriter::new();
        let mut c = TimestampStreamWriter::new(0);
        c.push(    1, &mut w); assert_eq!(w.string, "00000000000001");                                                                          // delta     1
        c.push(   51, &mut w); assert_eq!(w.string, "00000000000001101110000");                                                                 // delta    50, dod = 49
        c.push(  251, &mut w); assert_eq!(w.string, "00000000000001101110000110110010101");                                                     // delta   200, dod = 150
        c.push( 1251, &mut w); assert_eq!(w.string, "000000000000011011100001101100101011110101100011111");                                     // delta  1000, dod = 800
        c.push(11251, &mut w); assert_eq!(w.string, "000000000000011011100001101100101011110101100011111111100000000000000000010001100101000"); // delta 10000, dod = 9000

        let mut r = TimestampStreamIterator::new(StringReader::new(w.string), 0);
        assert_eq!(r.next(), Some(    1));
        assert_eq!(r.next(), Some(   51));
        assert_eq!(r.next(), Some(  251));
        assert_eq!(r.next(), Some( 1251));
        assert_eq!(r.next(), Some(11251));
        assert_eq!(r.next(), None);
    }

    #[test]
    fn fuzzer_vec_int () {
        // throw some random values at it and see if they decode correctly
        let header_time = 0;
        let mut w = VecWriter::new();
        let mut c = TimestampStreamWriter::new(header_time);
        let mut numbers = Vec::new();

        for i in header_time..1_000 {
            c.push(i, &mut w);
            numbers.push(i);
        }

        let r = TimestampStreamIterator::new(VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }

    #[test]
    fn time_and_value () {
        let header_time = 10000;
        let mut w = VecWriter::new();
        let mut c = TimeAndValueStream::new(header_time);

        let mut numbers = Vec::new();
        numbers.push((10005, 0.34f64));
        numbers.push((10065, 0.35f64));
        numbers.push((10124, 0.72f64));
        numbers.push((10247, 0.42f64));
        numbers.push((10365, 1.12f64));

        for &(timestamp, value) in numbers.iter() {
            c.push(timestamp, value, &mut w);
        }

        let r = TimeAndValueIterator::new(VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);

        for (from_vector, from_stream) in numbers.iter().zip(r) {
            assert_eq!(*from_vector, from_stream);
        }
    }
}
