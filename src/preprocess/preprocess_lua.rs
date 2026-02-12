use crate::data_source::{DataSourceRecord, DataSourceRecordIndex};
use crate::import_profile::import_profile_raw::PreprocessScript;
use crate::preprocess::{
    PreprocessFunctionError, PreprocessRuntime, PreprocessTransform, PreprocessTransformError,
};
use itertools::Itertools;
use itertools::Position;
use log::{debug, error, info, trace, warn};
use mlua::{AsChunk, ChunkMode, Function, IntoLua, Lua, Value, Variadic};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Result as IoResult;
use std::rc::Rc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadPreprocessRuntimeLuaError {
    #[error("could not configure lua")]
    Configure(#[source] mlua::Error),
    #[error("could not execute script")]
    Execute(#[source] mlua::Error),
}

#[derive(Debug)]
pub struct PreprocessLua {
    inner: Rc<Lua>,
}

#[derive(Debug)]
pub struct PreprocessLuaTransform {
    #[allow(dead_code, reason = "Lua runtime ends on drop")]
    inner: Rc<Lua>,
    function: Function,
}

macro_rules! register_log_function_lua {
    ($lua:expr, $log:ident) => {{
        let function = $lua.create_function(|lua, args: Variadic<Value>| {
            let tostring = lua.globals().get::<Function>("tostring")?;

            let mut buf = String::new();

            for (pos, arg) in args.into_iter().with_position() {
                if matches!(pos, Position::Middle | Position::Last) {
                    buf.push('\t');
                }

                buf.push_str(&tostring.call::<String>(arg)?);
            }

            $log!(target: "preprocess::lua", "{}", buf);

            Ok(())
        });

        match function {
            Ok(function) => {
                $lua.globals().set(stringify!($log), function)
            },
            Err(err) => Err(err)
        }
    }};
}

impl PreprocessLua {
    const PREPROCESS_SCRIPT_PREAMBLE_LUA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/assets/preprocess.luau"
    ));

    pub fn new(script: PreprocessScript) -> Result<Self, LoadPreprocessRuntimeLuaError> {
        let lua = Lua::new();

        lua.sandbox(true)
            .map_err(LoadPreprocessRuntimeLuaError::Configure)?;

        (|| {
            register_log_function_lua!(lua, error)?;
            register_log_function_lua!(lua, warn)?;
            register_log_function_lua!(lua, info)?;
            register_log_function_lua!(lua, debug)?;
            register_log_function_lua!(lua, trace)?;
            Ok(())
        })()
        .map_err(LoadPreprocessRuntimeLuaError::Configure)?;

        lua.load(Self::PREPROCESS_SCRIPT_PREAMBLE_LUA)
            .exec()
            .map_err(LoadPreprocessRuntimeLuaError::Configure)?;

        lua.load(script)
            .exec()
            .map_err(LoadPreprocessRuntimeLuaError::Execute)?;

        Ok(Self {
            inner: Rc::new(lua),
        })
    }
}

impl PreprocessRuntime for PreprocessLua {
    fn function(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn PreprocessTransform>>, PreprocessFunctionError> {
        match self.inner.globals().get::<Option<Function>>(name) {
            Ok(function) => Ok(function.map(|function| {
                let transform = PreprocessLuaTransform {
                    inner: self.inner.clone(),
                    function,
                };

                Box::new(transform) as Box<dyn PreprocessTransform>
            })),
            Err(err) => Err(err.into()),
        }
    }
}

impl PreprocessTransform for PreprocessLuaTransform {
    fn transform(
        &self,
        record: DataSourceRecord,
    ) -> Result<Option<DataSourceRecord>, PreprocessTransformError> {
        let index = record.index();

        let result = self
            .function
            .call::<Option<BTreeMap<String, String>>>((record, index))?;

        Ok(result.map(|fields| DataSourceRecord::from_iter(fields.into_iter(), index)))
    }
}

impl IntoLua for DataSourceRecord {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        Ok(Value::Table(lua.create_table_from(self.into_iter())?))
    }
}

impl IntoLua for DataSourceRecordIndex {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        let table = lua.create_table()?;

        table.set("record_number", self.record_number.get())?;
        table.set("line_start", self.line_start)?;
        table.set("line_end", self.line_end)?;

        Ok(Value::Table(table))
    }
}

impl AsChunk for PreprocessScript {
    fn name(&self) -> Option<String> {
        match self {
            PreprocessScript::File { path, .. } => path.name(),
            PreprocessScript::Inline { .. } => None,
        }
    }

    fn mode(&self) -> Option<ChunkMode> {
        match self {
            PreprocessScript::File { path, .. } => path.mode(),
            PreprocessScript::Inline { .. } => Some(ChunkMode::Text),
        }
    }

    fn source<'a>(&self) -> IoResult<Cow<'a, [u8]>>
    where
        Self: 'a,
    {
        match self {
            PreprocessScript::File { path, .. } => path.source(),
            PreprocessScript::Inline { script, .. } => script.source(),
        }
    }
}
