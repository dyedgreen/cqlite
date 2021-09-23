#[allow(unused_macros)]
macro_rules! assert_err {
    ($expr:expr, $err:pat) => {
        match $expr {
            Err($err) => (),
            _ => assert!(false, "Unexpected {}", $expr.err().unwrap()),
        }
    };
}
