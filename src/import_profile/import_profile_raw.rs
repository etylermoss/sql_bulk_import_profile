use crate::import_profile::ImportProfileDataSourceConfig;
use crate::table_mapper::table_mapper_raw::TableMapperRaw;
use schemars::JsonSchema;
use serde::Deserialize;
use std::fmt::Display;
use std::path::PathBuf;

#[derive(Debug, JsonSchema, Deserialize)]
#[serde(rename = "ImportProfile")]
pub struct ImportProfileRaw {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) data_source_config: ImportProfileDataSourceConfig,
    pub(crate) preprocess_script: Option<PreprocessScript>,
    pub(crate) table_mappers: Vec<TableMapperRaw>,
}

#[derive(Debug, JsonSchema, Deserialize)]
pub enum PreprocessScript {
    File {
        path: PathBuf,
        language: Option<PreprocessLanguage>,
    },
    Inline {
        script: String,
        language: PreprocessLanguage,
    },
}

#[derive(Debug, Copy, Clone, JsonSchema, Deserialize)]
pub enum PreprocessLanguage {
    #[cfg(feature = "lua")]
    Lua,
    #[cfg(feature = "rhai")]
    Rhai,
}

impl PreprocessScript {
    pub fn language(&self) -> Option<PreprocessLanguage> {
        match self {
            PreprocessScript::File { path, language } => match language {
                Some(language) => Some(*language),
                None => match path.extension()?.to_str()? {
                    #[cfg(feature = "lua")]
                    "lua" | "luau" => Some(PreprocessLanguage::Lua),
                    #[cfg(feature = "rhai")]
                    "rhai" => Some(PreprocessLanguage::Rhai),
                    _ => None,
                },
            },
            PreprocessScript::Inline { language, .. } => Some(*language),
        }
    }
}

impl Display for PreprocessScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreprocessScript::File { path, .. } => write!(f, "{}", path.display()),
            PreprocessScript::Inline { .. } => write!(f, "<inline>"),
        }
    }
}
