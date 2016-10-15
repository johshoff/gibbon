This is an incomplete implementation of the memory format of Facebook's Gorilla
database in Rust, as described in [Gorilla: A Fast, Scalable, In-Memory Time
Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

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
- IntStream writes the number as the two's compliment using the available bits.
  This is fast for writing, but requires extra logic for reading. Might be
  worth revisiting to see if read speed can be improved by doing the logic on
  the write side.

Further work
------------

- Measure and optimize performance
- Resolve open questions in _Implementation details_
- Implement the rest of the paper
- Investigate whether Rust's Write and Read traits could be used instead of hand rolled traits
- Separate IntStream and DoubleStream out in different files
