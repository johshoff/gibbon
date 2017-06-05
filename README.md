This is an incomplete implementation of the memory format of Facebook's Gorilla
database in Rust, as described in [Gorilla: A Fast, Scalable, In-Memory Time
Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

Example
-------

There is an example at [examples/csv_to_packed.rs](examples/csv_to_packed.rs).
Run as follows:

    cargo run --release --example csv_to_packed

This will read the file [examples/test_data.csv](examples/test_data.csv) and
compress it in memory. It's not a very interesting file, but replacing it with
your favorite data will show compression ratio and speed differences between
compressed and uncompressed reads.

There are also examples in the test code in the modules.

Implementation details
----------------------

The Gorilla Paper leaves some details out:

- The number of significant bits when compressing doubles are stored in a 6
  bits, giving a max value of 63. The key thing to notice is that only 63
  values are actually needed: 1 through 64. I solve this by storing `M - 1`,
  where `M` is the number of significant bits (`[MEANING64]` in code).
  [Another implementation](https://github.com/dgryski/go-tsz/blob/4815cfd89fc090a7bef4a8fc0cb0f5695a23ceaa/tsz.go#L135-L137)
  stores it by storing `M & 63` and resolving it at read time. Either solution
  is fine. The former optimizes for read speed and the latter for write speed.
- The number of leading zeros is stored in 5 bits, which gives a maximum of 31
  leading zeros. There is nothing preventing significant bits from having
  leading zeros, though, so we just use 31 if it's 31 or higher. (`[LEADING31]`
  in code)
- Leading number in previous XOR. Are we storing that or the XOR itself? If the
  former, the window will keep the same if we reuse it, if not it might shrink
  as new data comes in. Unsure about the best solution. (`[XORORLEADING]` in
  code)
- IntStream writes the number plus a bias so that the resulting number is
  always a non-negative number. This makes it fast to encode and decode without
  branching or being dependent on hardware representation of numbers. The
  initial version was not as smart and took about twice as long to decode.

Further work
------------

- Measure and optimize performance
- Resolve open questions in _Implementation details_
- Implement the rest of the paper
- Investigate whether Rust's Write and Read traits could be used instead of hand rolled traits
- Better naming:
    - `Stream` can now mean both bit-stream and compressed stream
    - `Writer` can refer both to the `Writer` trait and its `impl`s or to a "compressor"
