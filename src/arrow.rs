/// https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
#[derive(Debug, PartialEq)]
pub enum ArrowType {
    Null,
    Boolean,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float16,
    Float32,
    Float64,
    // Only implementing primitive types for now
}
impl ArrowType {
    fn from(format_str: &str) -> ArrowType {
        match format_str {
            "n" => ArrowType::Null,
            "b" => ArrowType::Boolean,
            "c" => ArrowType::Int8,
            "C" => ArrowType::Uint8,
            "s" => ArrowType::Int16,
            "S" => ArrowType::Uint16,
            "i" => ArrowType::Int32,
            "I" => ArrowType::Uint32,
            "l" => ArrowType::Int64,
            "L" => ArrowType::Uint64,
            "e" => ArrowType::Float16,
            "f" => ArrowType::Float32,
            "g" => ArrowType::Float64,
            &_ => todo!(),
        }
    }
}

/// https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure
#[repr(C)]
#[derive(Debug)]
pub struct ArrowCDataInterfaceArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const ::std::os::raw::c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut ArrowArray)>,
    pub private_data: *mut ::std::os::raw::c_void,
}

/// https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure
#[repr(C)]
#[derive(Debug)]
pub struct ArrowCDataInterfaceSchema {
    pub format: *const ::std::os::raw::c_char,
    pub name: *const ::std::os::raw::c_char,
    pub metadata: *const ::std::os::raw::c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowCDataInterfaceSchema,
    pub dictionary: *mut ArrowCDataInterfaceSchema,
    pub release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut ArrowCDataInterfaceSchema)>,
    pub private_data: *mut ::std::os::raw::c_void,
}
impl ArrowCDataInterfaceSchema {
    pub unsafe fn dtype(&self) -> ArrowType {
        let as_str = std::ffi::CStr::from_ptr(self.format).to_str().unwrap();
        ArrowType::from(as_str)
    }
}

pub struct ArrowArray {
    index: isize,
    length: usize,
    data_addr: usize,
    validity_addr: usize,
}
impl ArrowArray {
    pub unsafe fn from(array_pointer: *mut std::ffi::c_void,
                       schema_pointer: *mut std::ffi::c_void) -> ArrowArray {
        let interface_array = &mut *(array_pointer as *mut ArrowCDataInterfaceArray);
        let interface_schema = &mut *(schema_pointer as *mut ArrowCDataInterfaceSchema);

        if interface_schema.dtype() != ArrowType::Int64 {
            panic!("Extension only implemented for i64");
        }
        if interface_array.n_buffers != 2 {
            panic!("Extension only implemented for 2-buffer arrays, found {}",
                   interface_array.n_buffers);
        }

        return ArrowArray {
            index: 0,
            length: interface_array.length as usize,
            data_addr: *interface_array.buffers.offset(1) as usize,
            validity_addr: *interface_array.buffers as usize,
        };
    }
}
impl Iterator for ArrowArray {
    type Item = i64;

    /// Not really sure this approach is the best, but for now it works and seems to
    /// be fast. But it's surely worth trying other approaches, both for code clarity
    /// and for performance. I don't think parallelizing with this approach manually
    /// would be difficult, but I guess Rayon's `.par_iter()` won't work out of the
    /// box (I didn't try yet).
    fn next(&mut self) -> Option<i64> {
        if self.index >= self.length as isize {
            return None;
        }
        let base_ptr: *const i64 = unsafe { &*(self.data_addr as *const i64) };
        let next_value = unsafe { *base_ptr.offset(self.index) };
        self.index += 1;
        Some(next_value)
    }
}
