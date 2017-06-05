//! A timestamp stream contains integers and compresses well when they occur at
//! regular intervals. They are also tuned to work well for seconds. While for
//! example milliseconds would also work, it would compress poorly if not at an
//! exact interval.

pub use stream::{Writer, Reader};

pub enum TimestampStreamState {
    Initial {
        header_time: u64 // aligned to a two hour window
    },
    Following {
        value: u64,
        delta: i64,
    },
}

pub struct TimestampStreamWriter {
    state: TimestampStreamState
}

impl TimestampStreamWriter {
    pub fn new(header_time: u64) -> Self {
        TimestampStreamWriter {
            state: TimestampStreamState::Initial { header_time: header_time }
        }
    }

    pub fn push(&mut self, number: u64, writer: &mut Writer) {
        let delta = match self.state {
            TimestampStreamState::Initial { header_time } => {
                assert!(number >= header_time); // header time should be rounded down
                let delta = number - header_time;
                assert!(delta <= (1 << 14)); // enough to store more than four hours in seconds
                writer.write(delta, 14);

                delta as i64
            },
            TimestampStreamState::Following { value: prev_value, delta: prev_delta } => {
                let delta = (number - prev_value) as i64;
                let delta_of_deltas = delta - prev_delta;

                if delta_of_deltas == 0 {
                    writer.write(0, 1);
                } else if delta_of_deltas >= -63 && delta_of_deltas <= 64 {
                    writer.write(0b10, 2);
                    writer.write((delta_of_deltas + 63) as u64, 7);
                } else if delta_of_deltas >= -255 && delta_of_deltas <= 256 {
                    writer.write(0b110, 3);
                    writer.write((delta_of_deltas + 255) as u64, 9);
                } else if delta_of_deltas >= -2047 && delta_of_deltas <= 2048 {
                    writer.write(0b1110, 4);
                    writer.write((delta_of_deltas + 2047) as u64, 12);
                } else {
                    writer.write(0b1111, 4);
                    writer.write(delta_of_deltas as u64, 32);
                }

                delta
            }
        };

        self.state = TimestampStreamState::Following {
            value: number,
            delta: delta
        };
    }
}

pub struct TimestampStreamParser {
    state: TimestampStreamState,
}

impl TimestampStreamParser {
    pub fn new(header_time: u64) -> Self {
        TimestampStreamParser {
            state: TimestampStreamState::Initial { header_time: header_time }
        }
    }

    pub fn next(&mut self, reader: &mut Reader) -> Option<u64> {
        let values = match self.state {
            TimestampStreamState::Initial { header_time } => {
                reader.read(14).and_then(|delta| Some((header_time + delta, delta as i64)))
            }
            TimestampStreamState::Following { value, delta } => {
                match reader.read(1) {
                    Some(0) => Some((value.wrapping_add(delta as u64), delta)),
                    Some(1) => {
                        // unwrapping reads from now on, on the assumption that the stream is
                        // well-formed

                        let (num_bits, bias) = if reader.read(1).unwrap() == 0 { // 10
                            (7, 63)
                        } else if reader.read(1).unwrap() == 0 { // 110
                            (9, 255)
                        } else if reader.read(1).unwrap() == 0 { // 1110
                            (12, 2047)
                        } else { // 1111
                            (32, 0)
                        };

                        let delta_of_deltas = reader.read(num_bits).unwrap() as i64 - bias;

                        let new_delta = delta + delta_of_deltas;
                        let new_value = value.wrapping_add(new_delta as u64);
                        Some((new_value, new_delta))
                    }
                    None => None,
                    _ => panic!("Binary read should not be able to return anything but 0 or 1")
                }
            }
        };

        if let Some((value, delta)) = values {
            self.state = TimestampStreamState::Following { value: value, delta: delta };
            Some(value)
        } else {
            None
        }
    }
}

pub struct TimestampStreamIterator<R> where R: Reader {
    parser: TimestampStreamParser,
    reader: R,
}

impl<R> TimestampStreamIterator<R> where R: Reader {
    pub fn new(reader: R, header_time: u64) -> Self {
        TimestampStreamIterator {
            parser: TimestampStreamParser::new(header_time),
            reader: reader,
        }
    }
}

impl<R> Iterator for TimestampStreamIterator<R> where R: Reader {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.parser.next(&mut self.reader)
    }
}

