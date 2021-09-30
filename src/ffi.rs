use crate::parser;
use crate::planner::QueryPlan;
use crate::runtime::{Program, Status, VirtualMachine};
use crate::store::{Property, Store, StoreTxn};
use crate::Error;
use std::collections::HashMap;
use std::ffi::{c_void, CStr};
use std::os::raw::c_char;
use std::ptr::read;
use std::sync::atomic::{AtomicUsize, Ordering};

#[repr(u8)]
#[allow(non_camel_case_types)]
pub enum CQLiteStatus {
    CQLITE_OK,
    CQLITE_MATCH,
    CQLITE_DONE,

    // Errors
    CQLITE_IO,
    CQLITE_CORRUPTION,
    CQLITE_POISON,
    CQLITE_INTERNAL,
    CQLITE_READ_ONLY_WRITE,
    CQLITE_SYNTAX,
    CQLITE_IDENTIFIER_IS_NOT_NODE,
    CQLITE_IDENTIFIER_IS_NOT_EDGE,
    CQLITE_IDENTIGIER_EXISTS,
    CQLITE_UNKNOWN_IDENTIFIER,
    CQLITE_TYPE_MISMATCH,
    CQLITE_INDEX_OUT_OF_BOUNDS,
    CQLITE_MISSING_NODE,
    CQLITE_MISSING_EDGE,
    CQLITE_DELETE_CONNECTED,

    // FFI specific errors
    CQLITE_INVALID_STRING,
    CQLITE_OPEN_TRANSACTION,
    CQLITE_OPEN_STATEMENT,
    CQLITE_MISUSE,
}

#[repr(u8)]
#[allow(non_camel_case_types)]
pub enum CQLiteType {
    CQLITE_ID,
    CQLITE_INTEGER,
    CQLITE_REAL,
    CQLITE_BOOLEAN,
    CQLITE_TEXT,
    CQLITE_BLOB,
    CQLITE_NULL,
}

pub struct CQLiteGraph {
    store: Store,
    txn_count: AtomicUsize,
    stmt_count: AtomicUsize,
}

pub struct CQLiteTxn {
    graph: *const CQLiteGraph,
    txn: StoreTxn<'static>,
}

pub struct CQLiteStatement {
    graph: *const CQLiteGraph,
    program: *mut Program,
    parameters: HashMap<String, Property>,
    runtime: Option<(
        VirtualMachine<'static, 'static, 'static>,
        Vec<Option<Vec<u8>>>,
    )>,
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_open(
    path: *const c_char,
    graph: *mut *mut CQLiteGraph,
) -> CQLiteStatus {
    let inner = || -> Result<CQLiteGraph, CQLiteStatus> {
        let path = CStr::from_ptr(path)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        Ok(CQLiteGraph {
            store: Store::open(path)?,
            txn_count: AtomicUsize::new(0),
            stmt_count: AtomicUsize::new(0),
        })
    };
    match inner() {
        Err(err) => err,
        Ok(g) => {
            *graph = Box::into_raw(Box::new(g));
            CQLiteStatus::CQLITE_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_open_anon(graph: *mut *mut CQLiteGraph) -> CQLiteStatus {
    let inner = || -> Result<CQLiteGraph, CQLiteStatus> {
        Ok(CQLiteGraph {
            store: Store::open_anon()?,
            txn_count: AtomicUsize::new(0),
            stmt_count: AtomicUsize::new(0),
        })
    };
    match inner() {
        Err(err) => err,
        Ok(g) => {
            *graph = Box::into_raw(Box::new(g));
            CQLiteStatus::CQLITE_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_close(graph: *mut CQLiteGraph) -> CQLiteStatus {
    if (*graph).txn_count.load(Ordering::SeqCst) > 0 {
        CQLiteStatus::CQLITE_OPEN_TRANSACTION
    } else if (*graph).stmt_count.load(Ordering::SeqCst) > 0 {
        CQLiteStatus::CQLITE_OPEN_STATEMENT
    } else {
        drop(Box::from_raw(graph));
        CQLiteStatus::CQLITE_OK
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_txn(
    graph: *const CQLiteGraph,
    txn: *mut *mut CQLiteTxn,
) -> CQLiteStatus {
    let inner = || -> Result<CQLiteTxn, CQLiteStatus> {
        let txn = (*graph).store.txn()?;
        (*graph).txn_count.fetch_add(1, Ordering::SeqCst);
        Ok(CQLiteTxn { graph, txn })
    };
    match inner() {
        Err(err) => err,
        Ok(t) => {
            *txn = Box::into_raw(Box::new(t));
            CQLiteStatus::CQLITE_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_mut_txn(
    graph: *const CQLiteGraph,
    txn: *mut *mut CQLiteTxn,
) -> CQLiteStatus {
    let inner = || -> Result<CQLiteTxn, CQLiteStatus> {
        let txn = (*graph).store.mut_txn()?;
        (*graph).txn_count.fetch_add(1, Ordering::SeqCst);
        Ok(CQLiteTxn { graph, txn })
    };
    match inner() {
        Err(err) => err,
        Ok(t) => {
            *txn = Box::into_raw(Box::new(t));
            CQLiteStatus::CQLITE_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_drop(txn: *mut CQLiteTxn) -> CQLiteStatus {
    if !txn.is_null() {
        (*(*txn).graph).txn_count.fetch_sub(1, Ordering::SeqCst);
        drop(Box::from_raw(txn));
    }
    CQLiteStatus::CQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_commit(txn: *mut CQLiteTxn) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let txn = read(txn);
        txn.txn.commit()?;
        (*txn.graph).txn_count.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_prepare(
    graph: *const CQLiteGraph,
    query: *const c_char,
    stmt: *mut *mut CQLiteStatement,
) -> CQLiteStatus {
    let inner = || -> Result<CQLiteStatement, CQLiteStatus> {
        let query = CStr::from_ptr(query)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        let ast = parser::parse(query).map_err(|_| CQLiteStatus::CQLITE_SYNTAX)?;
        let plan = QueryPlan::new(&ast)?.optimize()?;
        let program = Box::into_raw(Box::new(Program::new(&plan)?));
        (*graph).stmt_count.fetch_add(1, Ordering::SeqCst);
        Ok(CQLiteStatement {
            graph,
            program,
            parameters: HashMap::new(),
            runtime: None,
        })
    };
    match inner() {
        Err(err) => err,
        Ok(s) => {
            *stmt = Box::into_raw(Box::new(s));
            CQLiteStatus::CQLITE_OK
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_start(
    stmt: *mut CQLiteStatement,
    txn: *mut CQLiteTxn,
) -> CQLiteStatus {
    (*stmt).runtime = Some((
        VirtualMachine::new(
            &mut txn.as_mut().unwrap().txn,
            (*stmt).program.as_mut().unwrap(),
            (*stmt).parameters.clone(),
        ),
        (*(*stmt).program).returns.iter().map(|_| None).collect(),
    ));
    CQLiteStatus::CQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_step(stmt: *mut CQLiteStatement) -> CQLiteStatus {
    if let Some((vm, buffers)) = (*stmt).runtime.as_mut() {
        let mut inner = || -> Result<CQLiteStatus, CQLiteStatus> {
            if (*(*stmt).program).returns.is_empty() {
                loop {
                    match vm.run()? {
                        Status::Yield => continue,
                        Status::Halt => break Ok(CQLiteStatus::CQLITE_DONE),
                    }
                }
            } else {
                buffers.iter_mut().for_each(|b| *b = None);
                match vm.run()? {
                    Status::Yield => Ok(CQLiteStatus::CQLITE_MATCH),
                    Status::Halt => Ok(CQLiteStatus::CQLITE_DONE),
                }
            }
        };
        match inner() {
            Err(err) => err,
            Ok(status) => status,
        }
    } else {
        CQLiteStatus::CQLITE_MISUSE
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_finalize(stmt: *mut CQLiteStatement) -> CQLiteStatus {
    if !stmt.is_null() {
        drop(Box::from_raw((*stmt).program));
        drop(Box::from_raw(stmt));
        (*(*stmt).graph).stmt_count.fetch_sub(1, Ordering::SeqCst);
    }
    CQLiteStatus::CQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_id(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: u64,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Id(value));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_integer(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: i64,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Integer(value));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_real(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: f64,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Real(value));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_boolean(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: bool,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Boolean(value));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_text(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: *const c_char,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        let value = CStr::from_ptr(value)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Text(value.to_string()));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_blob(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
    value: *const c_void,
    length: usize,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        let value = std::slice::from_raw_parts(value as *const u8, length);
        (*stmt)
            .parameters
            .insert(name.to_string(), Property::Blob(value.to_vec()));
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_bind_null(
    stmt: *mut CQLiteStatement,
    name: *const c_char,
) -> CQLiteStatus {
    let inner = || -> Result<(), CQLiteStatus> {
        let name = CStr::from_ptr(name)
            .to_str()
            .map_err(|_| CQLiteStatus::CQLITE_INVALID_STRING)?;
        (*stmt).parameters.remove(name);
        Ok(())
    };
    match inner() {
        Err(err) => err,
        Ok(()) => CQLiteStatus::CQLITE_OK,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_type(stmt: *mut CQLiteStatement, idx: usize) -> CQLiteType {
    let (vm, _) = (*stmt).runtime.as_mut().unwrap();
    match vm.access_return(idx).unwrap() {
        Property::Id(_) => CQLiteType::CQLITE_ID,
        Property::Integer(_) => CQLiteType::CQLITE_INTEGER,
        Property::Real(_) => CQLiteType::CQLITE_REAL,
        Property::Boolean(_) => CQLiteType::CQLITE_BOOLEAN,
        Property::Text(_) => CQLiteType::CQLITE_TEXT,
        Property::Blob(_) => CQLiteType::CQLITE_BLOB,
        Property::Null => CQLiteType::CQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_id(stmt: *mut CQLiteStatement, idx: usize) -> u64 {
    let (vm, _) = (*stmt).runtime.as_mut().unwrap();
    match vm.access_return(idx).unwrap() {
        Property::Id(id) => id,
        _ => panic!(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_integer(stmt: *mut CQLiteStatement, idx: usize) -> i64 {
    let (vm, _) = (*stmt).runtime.as_mut().unwrap();
    match vm.access_return(idx).unwrap() {
        Property::Integer(num) => num,
        _ => panic!(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_real(stmt: *mut CQLiteStatement, idx: usize) -> f64 {
    let (vm, _) = (*stmt).runtime.as_mut().unwrap();
    match vm.access_return(idx).unwrap() {
        Property::Real(num) => num,
        _ => panic!(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_boolean(stmt: *mut CQLiteStatement, idx: usize) -> bool {
    let (vm, _) = (*stmt).runtime.as_mut().unwrap();
    match vm.access_return(idx).unwrap() {
        Property::Boolean(val) => val,
        _ => panic!(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_text(
    stmt: *mut CQLiteStatement,
    idx: usize,
) -> *const c_char {
    let (vm, buffers) = (*stmt).runtime.as_mut().unwrap();
    match &buffers[idx] {
        Some(buffer) => buffer.as_ptr() as *const c_char,
        None => match vm.access_return(idx).unwrap() {
            Property::Text(string) => {
                let mut buf = string.into_bytes();
                buf.push(0);
                buffers[idx] = Some(buf);
                buffers[idx].as_ref().unwrap().as_ptr() as *const c_char
            }
            _ => panic!(),
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_blob(
    stmt: *mut CQLiteStatement,
    idx: usize,
) -> *const c_void {
    let (vm, buffers) = (*stmt).runtime.as_mut().unwrap();
    match &buffers[idx] {
        Some(buffer) => buffer.as_ptr() as *const c_void,
        None => match vm.access_return(idx).unwrap() {
            Property::Text(string) => {
                let buf = string.into_bytes();
                buffers[idx] = Some(buf);
                buffers[idx].as_ref().unwrap().as_ptr() as *const c_void
            }
            _ => panic!(),
        },
    }
}

#[no_mangle]
pub unsafe extern "C" fn cqlite_return_bytes(stmt: *mut CQLiteStatement, idx: usize) -> usize {
    let (_, buffers) = (*stmt).runtime.as_mut().unwrap();
    match &buffers[idx] {
        Some(buffer) => buffer.len(),
        None => 0,
    }
}

impl From<Error> for CQLiteStatus {
    fn from(err: Error) -> Self {
        match err {
            Error::IO(_) => CQLiteStatus::CQLITE_IO,
            Error::Corruption => CQLiteStatus::CQLITE_CORRUPTION,
            Error::Poison => CQLiteStatus::CQLITE_POISON,
            Error::Internal => CQLiteStatus::CQLITE_INTERNAL,
            Error::ReadOnlyWrite => CQLiteStatus::CQLITE_READ_ONLY_WRITE,
            Error::Syntax { .. } => CQLiteStatus::CQLITE_SYNTAX,
            Error::IdentifierIsNotNode(_) => CQLiteStatus::CQLITE_IDENTIFIER_IS_NOT_NODE,
            Error::IdentifierIsNotEdge(_) => CQLiteStatus::CQLITE_IDENTIFIER_IS_NOT_EDGE,
            Error::IdentifierExists(_) => CQLiteStatus::CQLITE_IDENTIGIER_EXISTS,
            Error::UnknownIdentifier(_) => CQLiteStatus::CQLITE_UNKNOWN_IDENTIFIER,
            Error::TypeMismatch => CQLiteStatus::CQLITE_TYPE_MISMATCH,
            Error::IndexOutOfBounds => CQLiteStatus::CQLITE_INDEX_OUT_OF_BOUNDS,
            Error::MissingNode => CQLiteStatus::CQLITE_MISSING_NODE,
            Error::MissingEdge => CQLiteStatus::CQLITE_MISSING_EDGE,
            Error::DeleteConnected => CQLiteStatus::CQLITE_DELETE_CONNECTED,
        }
    }
}
