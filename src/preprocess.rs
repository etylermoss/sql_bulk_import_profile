#[cfg(feature = "lua")]
mod preprocess_lua;
#[cfg(feature = "rhai")]
mod preprocess_rhai;

use crate::data_source::DataSourceRecord;
use crate::import_profile::import_profile_raw::PreprocessScript;
use log::error;
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadPreprocessRuntimeError {
    #[error("could not determine script language")]
    UnknownLanguage,
    #[cfg(feature = "lua")]
    #[error("could not load lua")]
    LoadLua(#[from] preprocess_lua::LoadPreprocessRuntimeLuaError),
    #[cfg(feature = "rhai")]
    #[error("could not load rhai")]
    LoadRhai(#[from] preprocess_rhai::LoadPreprocessRuntimeRhaiError),
}

pub fn load_preprocess_runtime(
    script: PreprocessScript,
) -> Result<Box<dyn PreprocessRuntime>, LoadPreprocessRuntimeError> {
    match script.language() {
        #[cfg(feature = "lua")]
        Some(crate::import_profile::import_profile_raw::PreprocessLanguage::Lua) => {
            Ok(Box::new(preprocess_lua::PreprocessLua::new(script)?))
        }
        #[cfg(feature = "rhai")]
        Some(crate::import_profile::import_profile_raw::PreprocessLanguage::Rhai) => {
            Ok(Box::new(preprocess_rhai::PreprocessRhai::new(script)?))
        }
        _ => Err(LoadPreprocessRuntimeError::UnknownLanguage),
    }
}

pub trait PreprocessRuntime: Debug {
    fn function(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn PreprocessTransform>>, PreprocessFunctionError>;
}

pub type PreprocessFunctionError = Box<dyn Error + Send + Sync + 'static>;

pub trait PreprocessTransform: Debug {
    fn transform(
        &self,
        record: DataSourceRecord,
    ) -> Result<Option<DataSourceRecord>, PreprocessTransformError>;
}

pub type PreprocessTransformError = Box<dyn Error + Send + Sync + 'static>;
