# Proof of concept for Arrow based UDFs

The idea of this repo is to experiment with UDFs for Arrow arrays.

It is still unclear to me how easy things can be made for UDF developers,
but at least for simple cases like map operations, I think we could provide
a macro that allows users to simply write something like this (and maybe use
generics):

```rust
use arrow_udf

#[arrow_udf::map]
fn remove_13(array_item: i64) -> i64 {
     if array_item == 13 {
         return 0;
     }
     array_item
}
```

There are some decisions made in this first POC, that don't necessarily need
to be the best, but seem reasonable so far:

**The Rust side is Python-agnostic.** We could use Pyo3 instead, but this
approach has the advantage of being reusable from other languages, C, Rust
itself, Python and any that support a FFI for the C ABI. Another advantage in
my opinion is that it simplifies the Rust part.

**The Python side uses ctypes.** So far `ctypes` seems to be reasonable and
simple, and being part of Python's standard library seems like an advantage.

**Arrow functionality implemented from scratch.** I don't think this will be
a good decision in the long term, as more data types are supported, but as a
start seems to be ok and make things simpler and more transparent in my opinion.

**Poor error handling and memory management.** For now I didn't focus too much
in safety. This implementation leaks, since the release of the C API interface
to Arrow is never called. The validity mask is ignored, and if a type different
than i64 is used it just panics. All this of course needs to be addressed later,
but I think at this point is an asset, as it makes code shorter and simpler, and
is easier to understand the approach.

## Installation

The Python part requires:

```
mamba install numpy pyarrow pandas polars
```

The Rust crate doesn't have dependencies, to complile use `--release` to make
benchmarks meaningful:

```
cargo build --release
```

To run the Python part, make sure that the directory of the Rust library is available:

```
LD_LIBRARY_PATH=target/release/ python pydistance/__init__.py
```

## Benchmarks

So far the Rust implementation doesn't use multithreading or SIMD. Benchmarks use
a simple sum of 1-D distances (the absolute value of the difference of two scalars).

While of course this is not a fair comparison, since the UDF here is a custom implementation
and others are using generic functions, results seem to be quite good for the UDF when
running something as simple as `(data - 2).abs().sum()`:

```
pandas numpy time:   0.26598167419433594 secs
pandas arrow time:   0.4156177043914795 secs
polars time:         0.30258917808532715 secs
Rust UDF time:       0.06415510177612305 secs
```
