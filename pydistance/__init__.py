import ctypes
import time

import numpy
import pandas
import polars


ARROW_ARRAY_STRUCT_SIZE = 80
ARROW_SCHEMA_STRUCT_SIZE = 72


@pandas.api.extensions.register_series_accessor('distance')
class PandasDistanceExtension:
    def __init__(self, series):
        self._series = series

    @staticmethod
    def _to_c_data_interface(arrow_array):
        """
        Expose the Arrow array with Arrow's C data interface.

        This involves:

        - Allocate memory for the C struct
        - Call PyArrow's `_export_to_c` function to fill the struct
        - Return a C pointer to the struct
        """
        array_buffer = ctypes.create_string_buffer(ARROW_ARRAY_STRUCT_SIZE)
        schema_buffer = ctypes.create_string_buffer(ARROW_SCHEMA_STRUCT_SIZE)

        arrow_array._export_to_c(out_ptr=ctypes.addressof(array_buffer),
                                 out_schema_ptr=ctypes.addressof(schema_buffer))

        array_pointer = ctypes.cast(ctypes.pointer(array_buffer),
                                    ctypes.c_void_p)
        schema_pointer = ctypes.cast(ctypes.pointer(schema_buffer),
                                     ctypes.c_void_p)

        return array_pointer, schema_pointer

    @staticmethod
    def _load_c_library():
        """
        Load the C dynamic library where the extension is implemented.

        The extension can be implemented in Rust, but needs to be compiled
        with the C-ABI (i.e. `crate-type=cdylib` and `pub extern "C" fn`).

        Currently this is looking in the default Rust target directories.
        """
        lib_name = 'libdistance.so'
        try:
            lib_distance = ctypes.cdll.LoadLibrary(lib_name)
        except OSError:
            raise OSError(
                f'Could not find dynamic library "{lib_name}". '
                'Please set the environment variable "LD_LIBRARY_PATH" '
                'to the directory containing the library in your environment.'
            )
        return lib_distance

    @property
    def _arrow_array(self):
        """
        Return the Arrow Array of the Series.

        This involves the next unwrapping:

        - Series contains a `SingleBlockManager` instance in its `_data` attribute
        - The block manager contains the `ArrowExtensionArray` in its `array` attribute
        - The extension array contains the PyArrow ChunkedArray in its `_data` attribute

        We make sure that the Series is a primitive numerical type based on Arrow,
        otherwise raise an exception.
        """
        if not isinstance(self._series.dtype,
                          pandas.core.arrays.arrow.dtype.ArrowDtype):
            raise TypeError(
                f'Extension `{self.__class__.__name__}` is only implemented for Arrow '
                'dtypes, try casting the Series with '
                '`.astype("{self._series.dtype.name}[pyarrow]")`'
            )
        if self._series.dtype.type not in (int, float):
            raise TypeError(
                f'Extension `{self.__class__.__name__}` is only implemented for '
                f'numerical types, "{self._series.dtype.type} found. '
                'Try casting to integer or float'
            )

        chunks = self._series._data.array._data.chunks

        if len(chunks) != 1:
            raise ValueError(
                f'Extension `{self.__class__.__name__}` is only implemented for '
                f'PyArrow arrays with 1 chunk, {len(chunks)} found.'
            )

        return chunks[0]

    def euclidean_1d(self, other):
        """
        Compute the distance between each element of the Series and the `other` point
        in a 1 dimensional array.

        In practice this is the absolute value of the difference:

        ```python
        >>> (series - other).abs()
        ```
        """
        array_pointer, schema_pointer = self._to_c_data_interface(self._arrow_array)
        lib_distance = self._load_c_library()
        result = lib_distance.euclidean_1d_scalar_sum.restype = ctypes.c_int64
        result = lib_distance.euclidean_1d_scalar_sum(array_pointer,
                                                      schema_pointer,
                                                      ctypes.c_int64(other))
        return result


if __name__ == '__main__':
    # Distances:
    # 1 and 2 -> 1
    # 2 and 2 -> 0
    # 4 and 2 -> 2
    # ------------
    # Total:     3
    #
    data_test = pandas.Series([1, 2, 4], dtype='int64[pyarrow]')
    result = data_test.distance.euclidean_1d(2)
    assert result == 3, f'Expected 3, found {result}'

    data_numpy = pandas.Series(numpy.random.randint(low=-2**32,
                                                    high=2**32,
                                                    size=100_000_000),
                               dtype='int64')
    data_arrow = data_numpy.astype('int64[pyarrow]')

    data_polars = polars.Series(data_arrow)

    start = time.time()
    pandas_numpy_result = (data_numpy - 2).abs().sum()
    pandas_numpy_duration = time.time() - start

    start = time.time()
    pandas_arrow_result = (data_arrow - 2).abs().sum()
    pandas_arrow_duration = time.time() - start

    start = time.time()
    polars_result = (data_polars - 2).abs().sum()
    polars_duration = time.time() - start

    start = time.time()
    rust_result = data_arrow.distance.euclidean_1d(2)
    rust_duration = time.time() - start

    print(f'Results: pandas_numpy={pandas_numpy_result} '
          f'pandas_arrow={pandas_arrow_result} '
          f'polars={polars_result} '
          f'rust={rust_result}')
    print(f'pandas numpy time: {pandas_numpy_duration} secs')
    print(f'pandas arrow time: {pandas_arrow_duration} secs')
    print(f'polars time: {polars_duration} secs')
    print(f'Rust UDF time: {rust_duration} secs')
