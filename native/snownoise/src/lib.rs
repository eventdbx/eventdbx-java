use snow::{Builder, HandshakeState, TransportState};
use std::ffi::c_void;
use std::sync::Mutex;

const PATTERN: &str = "Noise_NNpsk0_25519_ChaChaPoly_SHA256";

struct Session {
    handshake: Option<HandshakeState>,
    transport: Option<TransportState>,
}

// Keep sessions pinned; caller owns freeing.
#[no_mangle]
pub extern "C" fn snow_init(psk_ptr: *const u8, psk_len: usize) -> *mut c_void {
    let psk = unsafe { std::slice::from_raw_parts(psk_ptr, psk_len) };
    let params = match PATTERN.parse() {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let builder = Builder::new(params).psk(0, psk);
    let handshake = match builder.build_initiator() {
        Ok(h) => h,
        Err(_) => return std::ptr::null_mut(),
    };
    let session = Session {
        handshake: Some(handshake),
        transport: None,
    };
    Box::into_raw(Box::new(Mutex::new(session))) as *mut c_void
}

fn with_session<F, T>(handle: *mut c_void, f: F) -> Option<T>
where
    F: FnOnce(&mut Session) -> Option<T>,
{
    if handle.is_null() {
        return None;
    }
    let mutex = unsafe { &mut *(handle as *mut Mutex<Session>) };
    let mut guard = mutex.lock().ok()?;
    f(&mut guard)
}

#[no_mangle]
pub extern "C" fn snow_write_handshake(
    handle: *mut c_void,
    out_ptr: *mut u8,
    out_cap: usize,
) -> isize {
    with_session(handle, |session| {
        let hs = session.handshake.as_mut()?;
        let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_cap) };
        match hs.write_message(&[], out) {
            Ok(len) => Some(len as isize),
            Err(_) => Some(-1),
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn snow_read_handshake(handle: *mut c_void, in_ptr: *const u8, in_len: usize) -> isize {
    with_session(handle, |session| {
        let hs = session.handshake.as_mut()?;
        let incoming = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
        if hs.read_message(incoming, &mut []).is_err() {
            return Some(-1);
        }
        let hs = session.handshake.take()?;
        match hs.into_transport_mode() {
            Ok(t) => {
                session.transport = Some(t);
                Some(0)
            }
            Err(_) => Some(-1),
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn snow_write(
    handle: *mut c_void,
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
) -> isize {
    with_session(handle, |session| {
        let transport = session.transport.as_mut()?;
        let plaintext = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
        let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_cap) };
        match transport.write_message(plaintext, out) {
            Ok(len) => Some(len as isize),
            Err(_) => Some(-1),
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn snow_read(
    handle: *mut c_void,
    in_ptr: *const u8,
    in_len: usize,
    out_ptr: *mut u8,
    out_cap: usize,
) -> isize {
    with_session(handle, |session| {
        let transport = session.transport.as_mut()?;
        let ciphertext = unsafe { std::slice::from_raw_parts(in_ptr, in_len) };
        let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, out_cap) };
        match transport.read_message(ciphertext, out) {
            Ok(len) => Some(len as isize),
            Err(_) => Some(-1),
        }
    })
    .unwrap_or(-1)
}

#[no_mangle]
pub extern "C" fn snow_free(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    unsafe { let _ = Box::from_raw(handle as *mut Mutex<Session>); };
}
