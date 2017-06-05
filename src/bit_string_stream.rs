//! This is a stream for writing bits to a string string stream. Only for testing.

pub use stream::{Writer, Reader};

pub struct StringWriter {
    pub string: String
}

pub struct StringReader {
    pub string: String,
    pub position: usize,
}

impl StringWriter {
    pub fn new() -> Self {
        StringWriter {
            string: String::new()
        }
    }
}

impl StringReader {
    pub fn new(string: String) -> Self {
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

