//! What SQLite in the engine asks of its host, through the import
//! `plexus:store/host`, and the VFS `plexus`, SQLite's default here, which
//! keeps a file in the host's files when the host has one by that name
//! (in a page: the browser's file store, opened by the worker before the
//! component starts) and in memory when it has none.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use sqlite_wasm_rs::vfs::{
    AccessMode, FileKind, LockLevel, MemChunksFile, OpenOptions, OpenRequest, OpenedFile, OsCallback, SQLiteIoMethods, SQLiteVfs, SyncOptions, VfsError,
    VfsErrorCode, VfsFile, VfsResult, VfsStore, register_vfs,
};

use crate::plexus::store::host;

/// Register the VFS once, before the first connection.
pub(crate) fn install() {
    thread_local!(static DONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) });
    if DONE.replace(true) {
        return;
    }
    // SAFETY: one thread; the VFS and its data live as long as the instance.
    let registered = unsafe { register_vfs::<Io, Vfs>("plexus", Files::default(), true) };
    std::mem::forget(registered.expect("the VFS registers"));
}

fn fill(buf: &mut [u8]) {
    buf.copy_from_slice(&host::random(buf.len() as u32));
}

// What sqlite-wasm-rs links against when there is no wasm-bindgen.
#[unsafe(no_mangle)]
extern "C" fn rust_sqlite_wasm_host_sleep(_seconds: u64, _nanoseconds: u32) {}

#[unsafe(no_mangle)]
unsafe extern "C" fn rust_sqlite_wasm_host_random(buf: *mut u8, len: usize) -> usize {
    fill(unsafe { std::slice::from_raw_parts_mut(buf, len) });
    len
}

#[unsafe(no_mangle)]
unsafe extern "C" fn rust_sqlite_wasm_host_fill_entropy(buf: *mut u8, len: usize) -> i32 {
    fill(unsafe { std::slice::from_raw_parts_mut(buf, len) });
    0
}

#[unsafe(no_mangle)]
unsafe extern "C" fn rust_sqlite_wasm_host_epoch_timestamp_in_ms(out: *mut i64) -> i32 {
    unsafe { out.write(host::now()) };
    0
}

/// No time zone: SQLite's `localtime` modifier is refused.
#[unsafe(no_mangle)]
extern "C" fn rust_sqlite_wasm_host_localtime(_unix_seconds: i64, _out: *mut u8) -> i32 {
    1
}

/// The host's file, or one in memory shared by every open of its name.
#[derive(Clone)]
enum File {
    Host(u32),
    Memory(Rc<RefCell<MemChunksFile>>),
}

#[derive(Default)]
struct Files(RefCell<HashMap<String, File>>);

impl VfsFile for File {
    fn read(&mut self, buf: &mut [u8], offset: u64) -> VfsResult<usize> {
        match self {
            File::Host(f) => {
                let got = host::read(*f, offset, buf.len() as u32);
                buf[..got.len()].copy_from_slice(&got);
                Ok(got.len())
            }
            File::Memory(m) => m.borrow_mut().read(buf, offset),
        }
    }
    fn write(&mut self, buf: &[u8], offset: u64) -> VfsResult<()> {
        match self {
            File::Host(f) => {
                host::write(*f, offset, buf);
                Ok(())
            }
            File::Memory(m) => m.borrow_mut().write(buf, offset),
        }
    }
    fn truncate(&mut self, size: u64) -> VfsResult<()> {
        match self {
            File::Host(f) => {
                host::truncate(*f, size);
                Ok(())
            }
            File::Memory(m) => m.borrow_mut().truncate(size),
        }
    }
    fn sync(&mut self, _: SyncOptions) -> VfsResult<()> {
        if let File::Host(f) = self {
            host::flush(*f);
        }
        Ok(())
    }
    fn size(&self) -> VfsResult<u64> {
        match self {
            File::Host(f) => Ok(host::size(*f)),
            File::Memory(m) => m.borrow().size(),
        }
    }
    // One connection in one worker: nothing to coordinate.
    fn lock(&mut self, _: LockLevel) -> VfsResult<()> {
        Ok(())
    }
    fn unlock(&mut self, _: LockLevel) -> VfsResult<()> {
        Ok(())
    }
    fn check_reserved_lock(&self) -> VfsResult<bool> {
        Ok(false)
    }
}

struct Store;

impl VfsStore for Store {
    type File = File;
    type AppData = Files;

    fn open_file(files: &Files, request: OpenRequest<'_>) -> VfsResult<OpenedFile<File>> {
        let access = request.options.access();
        let Some(name) = request.filename.map(|f| f.path().to_owned()) else {
            return Ok(OpenedFile { file: File::Memory(Default::default()), access });
        };
        let main = request.options.kind() == Some(FileKind::MainDb);
        let mut files = files.0.borrow_mut();
        let file = files
            .entry(name)
            .or_insert_with_key(|name| match host::open(name) {
                Some(f) => File::Host(f),
                None if main => File::Memory(Rc::new(RefCell::new(MemChunksFile::waiting_for_write()))),
                None => File::Memory(Default::default()),
            })
            .clone();
        Ok(OpenedFile { file, access })
    }

    fn close_file(files: &Files, name: Option<&str>, file: File, options: OpenOptions) -> VfsResult<()> {
        match (name, file) {
            (Some(name), File::Memory(_)) if options.delete_on_close() => {
                files.0.borrow_mut().remove(name);
            }
            (_, File::Host(f)) if options.delete_on_close() => host::truncate(f, 0),
            _ => {}
        }
        Ok(())
    }

    /// A host file is always there; empty, it is as good as gone.
    fn access(files: &Files, name: &str, _: AccessMode) -> VfsResult<bool> {
        Ok(match files.0.borrow().get(name) {
            Some(File::Host(f)) => host::size(*f) > 0,
            Some(File::Memory(_)) => true,
            None => host::open(name).is_some_and(|f| host::size(f) > 0),
        })
    }

    fn full_pathname(_: &Files, name: &str) -> VfsResult<String> {
        Ok(name.into())
    }

    fn delete_file(files: &Files, name: &str, _: bool) -> VfsResult<()> {
        match files.0.borrow_mut().remove(name) {
            Some(File::Host(f)) => host::truncate(f, 0),
            Some(File::Memory(_)) => {}
            None => return Err(VfsError::new(VfsErrorCode::IoDelete, format!("no file {name:?}").into())),
        }
        Ok(())
    }
}

struct Io;

impl SQLiteIoMethods for Io {
    type Store = Store;
}

struct Os;

impl OsCallback for Os {
    fn sleep(&self, _: std::time::Duration) {}
    fn random(&self, buf: &mut [u8]) -> usize {
        fill(buf);
        buf.len()
    }
    fn epoch_timestamp_in_ms(&self) -> VfsResult<i64> {
        Ok(host::now())
    }
}

struct Vfs;

impl SQLiteVfs<Io> for Vfs {
    type Os = Os;
    fn os(_: &Files) -> &Os {
        &Os
    }
}
