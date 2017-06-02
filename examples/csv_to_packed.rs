extern crate csv;
extern crate gibbon;
extern crate time;

use gibbon::*;
use std::{f64, u64};
use std::cmp;

struct TimeAndValue {
    timestamp: u64,
    value: f64,
}

fn main() {
    let mut rdr = csv::Reader::from_file("./examples/test_data.csv").unwrap();
    let mut w = vec_stream::VecWriter::new();
    let header_time = (1496366523 / 3600) * 3600;
    let mut c = TimeAndValueStream::new(header_time);

    let mut uncompressed = Vec::<TimeAndValue>::new();

    let mut start = time::precise_time_ns();
    for record in rdr.decode() {
        let (timestamp, value): (u64, f64) = record.unwrap();
        c.push(timestamp, value, &mut w);
        uncompressed.push(TimeAndValue { timestamp: timestamp, value: value });
    }
    let now = time::precise_time_ns();
    println!("Read data in {} ms", (now - start) as f64 / 1_000_000f64);
    start = now;

    //------------------------------------------------------

    println!("\nCompressed:");

    {
        let i = TimeAndValueIterator::new(vec_stream::VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);
        print!("Max: {}", i.map(|(_timestamp, value)| value).fold(f64::NEG_INFINITY, f64::max));
    }
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    {
        let i = TimeAndValueIterator::new(vec_stream::VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);
        print!("Min: {}", i.map(|(_timestamp, value)| value).fold(f64::INFINITY, f64::min));
    }
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    let samples = {
        let i = TimeAndValueIterator::new(vec_stream::VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);
        let samples = i.count();
        print!("Samples: {}", samples);
        samples
    };
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    {
        let i = TimeAndValueIterator::new(vec_stream::VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);
        print!("Average: {}", i.map(|(_timestamp, value)| value).sum::<f64>() / (samples as f64));
    }
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    {
        let i = TimeAndValueIterator::new(vec_stream::VecReader::new(&w.bit_vector, w.used_bits_last_elm), header_time);
        print!("Max timestamp: {}", i.map(|(timestamp, _value)| timestamp).fold(u64::MIN, cmp::max));
    }
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);

    //------------------------------------------------------

    println!("\nUncompressed:");

    print!("Max: {}", uncompressed.iter().map(|v| v.value).fold(f64::NEG_INFINITY, f64::max));
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    print!("Min: {}", uncompressed.iter().map(|v| v.value).fold(f64::INFINITY, f64::min));
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    print!("Samples: {}", uncompressed.iter().count());
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    print!("Average: {}", uncompressed.iter().map(|v| v.value).sum::<f64>() / (samples as f64));
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);
    start = now;

    print!("Max timestamp: {}", uncompressed.iter().map(|v| v.timestamp).fold(u64::MIN, cmp::max));
    let now = time::precise_time_ns();
    println!(" ({} ms)", (now - start) as f64 / 1_000_000f64);

    //------------------------------------------------------

    println!("\nStats:");

    let bytes_compressed = w.bit_vector.len() * 8;
    let bytes_uncompressed = uncompressed.len() * (8 + 8);
    println!("Bytes consumed             {:10}", bytes_compressed);
    println!("Bytes consumed uncompressed{:10}", bytes_uncompressed);
    println!("Compression ratio          {:10.2}%", (100f64 * (bytes_compressed as f64) / (bytes_uncompressed as f64)));
}

