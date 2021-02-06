use std::{
    ffi::CStr,
    fs::File,
    io::Write,
    os::raw::c_char,
    ptr,
};
use zstd::stream::write::Encoder;

#[repr(C)]
#[derive(PartialEq, Clone, Debug)]
pub struct ZstdWriter {
}

#[no_mangle]
pub extern "C" fn zstd_writer_open(zstd_file_path: *const c_char, level: i32) -> *mut ZstdWriter {

    let zstd_file_path = unsafe { CStr::from_ptr(zstd_file_path) };
    let file = File::create(zstd_file_path.to_str().unwrap());
    if file.is_err() {
        eprintln!("Cannot open file {}", zstd_file_path.to_str().unwrap());
        return ptr::null_mut::<ZstdWriter>()
    }
    let file = file.unwrap();
    let encoder = Encoder::new(file, level);
    if encoder.is_err() {
	eprintln!("Cannot create ZSTD encoder");
	return ptr::null_mut::<ZstdWriter>();
    }
    let encoder: Encoder<File> = encoder.unwrap();
    Box::into_raw(Box::new(encoder)) as *mut ZstdWriter
}

#[no_mangle]
pub extern "C" fn zstd_writer_write(writer: *mut ZstdWriter, content: *const u8, len: usize) -> i32 {
    unsafe {
	let encoder: *mut Encoder<File> = writer as *mut Encoder<File>;
	let content = std::ptr::slice_from_raw_parts(content, len);
	let content_ref: &[u8] = &(*content);
	 match (*encoder).write_all(content_ref) {
	    Ok(_) => { 
		0
	    },
	    Err(e) => {
		eprintln!("Cannot write to ZSTD writer: {:?}", e);
		-1
	    },
	}
    }
}

#[no_mangle]
pub extern "C" fn zstd_writer_close(writer: *mut ZstdWriter) -> i32 {
    unsafe {
	let encoder: *mut Encoder<File> = writer as *mut Encoder<File>;
	let encoder = Box::from_raw(encoder);
	match encoder.finish() {
	    Ok(_) => {
		0
	    },
	    Err(e) => {
		eprintln!("Cannot finish encoding ZSTD: {:?}", e);
		-1
	    }
	}
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::io::{BufRead, BufReader};
    use zstd::stream::read::Decoder;

    use super::*;
    #[test]
    fn test_wrapper_basic() {
	let buf = CString::new("/tmp/test.txt.zstd").unwrap();
	let file_path = buf.as_ptr();
	let writer = zstd_writer_open(file_path, 3);
	assert!(!writer.is_null());
	assert_eq!(zstd_writer_write(writer, "AB\n".as_bytes().as_ptr(), "AB\n".as_bytes().len()), 0);
	assert_eq!(zstd_writer_write(writer, "\n".as_bytes().as_ptr(), "\n".as_bytes().len()), 0);
	assert_eq!(zstd_writer_write(writer, "".as_bytes().as_ptr(), 0), 0);
	assert_eq!(zstd_writer_write(writer,
				     "กาก้า\n".as_bytes().as_ptr(),
				     "กาก้า\n".as_bytes().len()),
		   0);
	zstd_writer_close(writer);
	
	let f = File::open("/tmp/test.txt.zstd").unwrap();
	let decoder = Decoder::new(f).unwrap();
	let reader = BufReader::new(decoder);
	let mut lines = reader.lines();
	assert_eq!(lines.next().unwrap().unwrap(), String::from("AB"));
	assert_eq!(lines.next().unwrap().unwrap(), String::from(""));
	assert_eq!(lines.next().unwrap().unwrap(), String::from("กาก้า"));
	assert!(lines.next().is_none());
    }
}
