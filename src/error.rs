#[derive(Debug, PartialEq)]
pub enum Error {
    Todo,
}

impl<E: std::error::Error> From<E> for Error {
    fn from(error: E) -> Self {
        eprintln!("TODO: {:?}", error);
        Self::Todo
    }
}
