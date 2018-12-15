error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    errors {
        NotAnArchive(err: String) {
            description("given input is not a bita archive"),
            display("{}", err),
        }
    }
}
