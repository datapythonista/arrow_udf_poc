mod arrow;

/// Compute the Euclidean distance between two points in a 1-dimensional space.
///
/// In practice, this is just the absolute value of the difference.
///
/// For now, only implementing a version that computes the distance between an
/// Arrow array and a scalar, and returns the total sum.
/// This is for simplicity, to avoid in a first version to receive two different
/// Arrow arrays, or to have to return a new Arrow array, which would make the code
/// more difficult to follow. But these will be implemented later.
#[no_mangle]
pub unsafe extern "C" fn euclidean_1d_scalar_sum(
        array_pointer: *mut std::ffi::c_void,
        schema_pointer: *mut std::ffi::c_void,
        other: i64) -> i64  {

    let arrow_array = arrow::ArrowArray::from(array_pointer, schema_pointer);

    let mut accumulator = 0;
    let start = std::time::Instant::now();
    for item in arrow_array {
        accumulator = reduction_udf(accumulator, item, other);
    }
    println!("Rust loop time: {} secs", start.elapsed().as_micros() as f64 / 1e6);
    accumulator
}

/// This is where the UDF logic lives.
///
/// The idea is that users who want to implement their own UDF, just need to
/// write their logic in a function like this, and wrap it in a Rust macro
/// still not implemented. Something like:
///
/// ```rust
/// #[arrow_udf::map]
/// fn remove_13(array_item: i64) -> i64 {
///     if array_item == 13 {
///         return 0;
///     }
///     array_item
/// }
/// ```
///
/// The macro would take care of taking the Arrow array input, looping over it,
/// and calling the UDF. It could also run this in parallel.
#[inline]
fn reduction_udf(accumulator: i64, array_item: i64, other: i64) -> i64 {
    accumulator + (array_item - other).abs()
}
