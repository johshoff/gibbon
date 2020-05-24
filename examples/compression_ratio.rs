extern crate csv;
extern crate gibbon;
extern crate time;
extern crate libwhisper;

use gibbon::*;
use std::{f64, u64, u32};
use std::env;
use std::iter::Iterator;
use libwhisper::read_file;

use std::fs::File;
use std::io::Write;

fn main() -> Result<(), std::io::Error> {
    let args = env::args().skip(1);

    if args.len() == 0 {
        println!("compression_ratio <whisper_file>*");
        return Ok(());
    }

    println!("filename\tseconds per point\tpoints\ttimestamp compression\tvalue 1\tvalue 2");

    for filename in args {
        match read_file(&filename) {
            Ok(db) => {
                for (archive, info) in db.archives.iter().zip(db.header.archive_info) {
                    // TODO: This should really be in libwhisper
                    let (first_index, first_timestamp) = {
                        let mut first_index = None;
                        let mut min_timestamp = u32::MAX;
                        for (index, point) in archive.iter().enumerate() {
                            if point.timestamp != 0 && point.timestamp < min_timestamp {
                                first_index = Some(index);
                                min_timestamp = point.timestamp;
                            }
                        }
                        if let Some(first_index) = first_index {
                            (first_index, min_timestamp)
                        } else {
                            continue;
                        }
                    };

                    let header_time = ((first_timestamp / 3600) * 3600) as u64;
                    let mut timestamp_writer = TimestampStreamWriter::new(header_time);
                    let mut timestamp_buffer = vec_stream::VecWriter::new();

                    let mut value_writer = DoubleStreamWriter::new();
                    let mut value_buffer = vec_stream::VecWriter::new();

                    let mut value2_writer = DoubleStreamLeadTrail::new();
                    let mut value2_buffer = vec_stream::VecWriter::new();

                    let mut points = 0;

                    let mut i = first_index;

                    let out_filename = format!("{}-{}.json", filename, info.seconds_per_point);
                    let mut outfile = File::create(out_filename)?;
                    writeln!(outfile, "[")?;

                    let mut last = None;
                    let mut is_first = true;
                    loop {
                        let point = archive.get(i).unwrap();
                        if point.timestamp != 0 {
                            if let Some(last_num) = last {
                                if last_num > point.timestamp {
                                    //println!("yipes");
                                    //println!("");
                                    break;
                                }
                            }
                            //println!("{} timestamp: {}, {}", i, point.timestamp, point.value);
                            writeln!(outfile, "  {}{{\"date\":{}, \"value\":{}}}", (if is_first { "" } else { "," }), point.timestamp, point.value)?;
                            is_first = false;
                            last = Some(point.timestamp);
                            timestamp_writer.push(point.timestamp as u64, &mut timestamp_buffer);
                            value_writer.push(point.value, &mut value_buffer);
                            value2_writer.push(point.value, &mut value2_buffer);
                            points += 1;
                        }

                        i += 1;

                        if i == archive.len() {
                            i = 0;
                        }

                        if i == first_index {
                            break;
                        }
                    }

                    writeln!(outfile, "]")?;

                    let timestamps_compressed = timestamp_buffer.bit_vector.len() * 8;
                    let timestamps_uncompressed = points * 4;

                    let values_compressed = value_buffer.bit_vector.len() * 8;
                    let values2_compressed = value2_buffer.bit_vector.len() * 8;
                    let values_uncompressed = points * 8;

                    println!("{}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}",
                        filename,
                        info.seconds_per_point,
                        points,
                        (100f64 * (timestamps_compressed as f64) / (timestamps_uncompressed as f64)),
                        (100f64 * (values_compressed as f64) / (values_uncompressed as f64)),
                        (100f64 * (values2_compressed as f64) / (values_uncompressed as f64))
                        );
                    //println!("Compression                {:10.2}% of original", (100f64 * (bytes_compressed as f64) / (bytes_uncompressed as f64)));

                }
            }
            Err(err) => println!("Could not read file {}: {}", filename, err)
        }
    }

    Ok(())
}

