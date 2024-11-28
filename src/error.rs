use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    ObjectNotFound,

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },

    FailedToGetDatabaseSize {
        msg: String,
    },
}

impl Error {
    pub fn from_aws<E, R>(err: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        Self::Whatever {
            message: err.to_string(),
            source: None,
        }
    }
}

impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for Error {
    fn from(source: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        Self::Whatever {
            message: source.to_string(),
            source: None,
        }
    }
}
