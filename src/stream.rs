pub trait Writer {
    /// write the `count` least significant bits of `bits`
    fn write(&mut self, bits: u64, count: u8);
}

pub trait Reader {
    fn read(&mut self, count: u8) -> Option<u64>;
}

