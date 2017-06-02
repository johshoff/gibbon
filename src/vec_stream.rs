pub use stream::{Writer, Reader};

pub struct VecWriter {
    pub bit_vector: Vec<u64>,
    pub used_bits_last_elm: u8,
}

impl VecWriter {
    pub fn new() -> Self {
        VecWriter {
            bit_vector: vec![0u64],
            used_bits_last_elm: 0,
        }
    }

    pub fn len(&self) -> usize {
        (self.bit_vector.len() - 1) * 64 + (self.used_bits_last_elm as usize)
    }
}

impl Writer for VecWriter {
    fn write(&mut self, bits: u64, count: u8) {
        if self.used_bits_last_elm == 64 {
            self.bit_vector.push(bits << (64 - count));
            self.used_bits_last_elm = count;
        } else {
            let remaining_bits = 64 - self.used_bits_last_elm;
            if count <= remaining_bits {
                let last = self.bit_vector.last_mut().unwrap();
                *last ^= bits << (remaining_bits - count);
                self.used_bits_last_elm += count;
            } else {
                let bits_former = remaining_bits;
                let bits_latter = count - remaining_bits;

                {
                    let former = self.bit_vector.last_mut().unwrap();
                    *former ^= bits >> (count - bits_former);
                }

                self.bit_vector.push(bits << (64 - bits_latter));
                self.used_bits_last_elm = bits_latter;
            }
        }
    }
}

pub struct VecReader<'a> {
    bit_vector: &'a Vec<u64>,
    index: usize,
    read_bits_current_index: u8,
    num_bits_last_elm: u8,
}

impl<'a> VecReader<'a> {
    pub fn new(data: &'a Vec<u64>, num_bits_last_elm: u8) -> Self {
        VecReader {
            bit_vector: data,
            index: 0,
            read_bits_current_index: 0,
            num_bits_last_elm: num_bits_last_elm,
        }
    }
}

impl<'a> Reader for VecReader<'a> {
    fn read(&mut self, count: u8) -> Option<u64> {
        if self.bit_vector.len() == 0 {
            // TODO: would be nice if we could avoid this test every time. See
            // `read_empty_vector` for when it's needed.
            return None;
        }

        let total_bits = (self.bit_vector.len() - 1) * 64 + self.num_bits_last_elm as usize;
        let read_bits = self.index * 64 + self.read_bits_current_index as usize;
        let remaining_bits = total_bits - read_bits;

        if remaining_bits < count as usize {
            None
        } else {
            let remaining_bits_current_word = 64 - self.read_bits_current_index;
            if count <= remaining_bits_current_word {
                self.read_bits_current_index += count;
                let mask = 0xFFFFFFFFFFFFFFFF >> (64 - count);
                Some((self.bit_vector[self.index] >> (remaining_bits_current_word - count) & mask))
            } else {
                let bits_former = remaining_bits_current_word;
                let bits_latter = count - remaining_bits_current_word;

                self.index += 1;
                self.read_bits_current_index = bits_latter;

                Some(
                    ((self.bit_vector[self.index - 1] & ((1 << bits_former) - 1)) << bits_latter) +
                    (self.bit_vector[self.index] >> (64 - bits_latter))
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_nothing() {
        let w = VecWriter::new();
        assert_eq!(w.len(), 0);
    }

    #[test]
    fn write_to_first_word() {
        let mut w = VecWriter::new();
        w.write(1,  1); assert_eq!(w.len(),  1); assert_eq!(w.bit_vector[0], 0b1000000000000000000000000000000000000000000000000000000000000000);
        w.write(1,  1); assert_eq!(w.len(),  2); assert_eq!(w.bit_vector[0], 0b1100000000000000000000000000000000000000000000000000000000000000);
        w.write(1,  2); assert_eq!(w.len(),  4); assert_eq!(w.bit_vector[0], 0b1101000000000000000000000000000000000000000000000000000000000000);
        w.write(1,  6); assert_eq!(w.len(), 10); assert_eq!(w.bit_vector[0], 0b1101000001000000000000000000000000000000000000000000000000000000);
        w.write(1, 20); assert_eq!(w.len(), 30); assert_eq!(w.bit_vector[0], 0b1101000001000000000000000000010000000000000000000000000000000000);
        w.write(1, 34); assert_eq!(w.len(), 64); assert_eq!(w.bit_vector[0], 0b1101000001000000000000000000010000000000000000000000000000000001);
    }

    #[test]
    fn write_to_second_word_aligned() {
        let mut w = VecWriter::new();
        w.write(1, 64); assert_eq!(w.len(),  64); assert_eq!(w.bit_vector[0], 0b0000000000000000000000000000000000000000000000000000000000000001);
        w.write(2, 64); assert_eq!(w.len(), 128); assert_eq!(w.bit_vector[1], 0b0000000000000000000000000000000000000000000000000000000000000010);
    }

    #[test]
    fn write_to_second_word_unaligned() {
        let mut w = VecWriter::new();
        w.write(0, 62); assert_eq!(w.len(), 62); assert_eq!(w.bit_vector[0], 0b0000000000000000000000000000000000000000000000000000000000000000);
        w.write(0b10010, 5);
        assert_eq!(w.len(), 67);
        assert_eq!(w.bit_vector[0], 0b0000000000000000000000000000000000000000000000000000000000000010);
        assert_eq!(w.bit_vector[1], 0b0100000000000000000000000000000000000000000000000000000000000000);
    }

    #[test]
    fn read_first_word() {
        let data = vec![0b1101000001000000000000000000010000000000000000000000000000000001];
        let mut r = VecReader::new(&data, 64);
        assert_eq!(r.read(4), Some(0b1101));
        assert_eq!(r.read(4), Some(0b0000));
        assert_eq!(r.read(1), Some(0b0));
        assert_eq!(r.read(1), Some(0b1));
        assert_eq!(r.read(1), Some(0b0));
        assert_eq!(r.read(53), Some(0b00000000000000000010000000000000000000000000000000001));
        assert_eq!(r.read(1), None);
        assert_eq!(r.read(2), None);
        assert_eq!(r.read(4), None);
        assert_eq!(r.read(8), None);
    }

    #[test]
    fn read_unaligned_word() {
        let p = 0b1101000001000000000000000000010000000000000000000000000000000001;
        let data = vec![p, p];
        let mut r = VecReader::new(&data, 64);
        assert_eq!(r.read(63), Some(p >> 1));
        assert_eq!(r.read(5), Some(0b11101));
    }

    #[test]
    fn read_empty_vector() {
        let data = vec![];
        let mut r = VecReader::new(&data, 0);
        assert_eq!(r.read(1), None);
    }
}
