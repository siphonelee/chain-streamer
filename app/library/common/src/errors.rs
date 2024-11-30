use failure::{Backtrace, Fail};
use std::fmt;

#[derive(Debug)]
pub struct AuthError {
    pub value: AuthErrorValue,
}

#[derive(Debug, Fail)]
pub enum AuthErrorValue {
    #[fail(display = "token is not correct.")]
    TokenIsNotCorrect,
    #[fail(display = "no token found.")]
    NoTokenFound,
    #[fail(display = "invalid token format.")]
    InvalidTokenFormat
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl Fail for AuthError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.value.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.value.backtrace()
    }
}

#[derive(Debug)]
pub struct SuiError {
    pub value: SuiErrorValue,
}

#[derive(Debug, Fail)]
pub enum SuiErrorValue {
    #[fail(display = "sui client setup error")]
    SetupSuiClientError,
    #[fail(display = "get sui coin error")]
    GetSuiCoinError,
    #[fail(display = "PTB input error")]
    PTBInputError,
    #[fail(display = "identifier format error")]
    IdentifierFormatError,
    #[fail(display = "sui RPC error")]
    SuiRPCError,
    #[fail(display = "transaction sign error")]
    TransactionSignError,
    #[fail(display = "sui config error")]
    SuiConfigError,
    #[fail(display = "key store error")]
    FileKeyStoreError,
    #[fail(display = "transaction block execute error")]
    TransactionBlockExecuteError,
    #[fail(display = "parse error")]
    ParseError,
    #[fail(display = "sui ptb object error")]
    PTBObjError,
    #[fail(display = "json parse error")]
    JsonParseError,
}

impl fmt::Display for SuiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl Fail for SuiError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.value.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.value.backtrace()
    }
}
