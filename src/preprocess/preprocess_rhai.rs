use crate::data_source::DataSourceRecord;
use crate::import_profile::import_profile_raw::PreprocessScript;
use crate::preprocess::{
    PreprocessFunctionError, PreprocessRuntime, PreprocessTransform, PreprocessTransformError,
};
use log::{debug, error, info, trace, warn};
use rhai::{AST, Dynamic, Engine, EvalAltResult, FnAccess, Map, ParseError, Scope};
use std::cell::RefCell;
use std::rc::Rc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LoadPreprocessRuntimeRhaiError {
    #[error("could not configure rhai")]
    Configure(#[source] Box<EvalAltResult>),
    #[error("could not execute script")]
    Execute(#[source] Box<EvalAltResult>),
    #[error("could not parse script")]
    Parse(#[source] ParseError),
}

#[derive(Debug, Error)]
pub enum PreprocessTransformRhaiError {
    #[error("could not transform record with rhai")]
    Execute(
        #[from]
        #[source]
        Box<EvalAltResult>,
    ),
    #[error("rhai transform function returned an unexpected type '{0}'")]
    ResultNotMap(String),
    #[error("rhai transform function field '{field}' is an unexpected type '{type_name}'")]
    FieldNotString { field: String, type_name: String },
}

macro_rules! register_log_function_rhai {
    ($engine:expr, $log:ident) => {
        $engine.register_fn(stringify!($log), |arg: Dynamic| {
            $log!(target: "preprocess::rhai", "{}", arg);
        })
    };
}

#[derive(Debug)]
struct RhaiInner {
    engine: Box<Engine>,
    scope: RefCell<Scope<'static>>,
    ast: AST,
}

#[derive(Debug)]
pub struct PreprocessRhai {
    inner: Rc<RhaiInner>,
}

#[derive(Debug)]
pub struct PreprocessRhaiTransform {
    inner: Rc<RhaiInner>,
    function: String,
}

impl PreprocessRhai {
    const PREPROCESS_SCRIPT_PREAMBLE_RHAI: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/assets/preprocess.rhai"
    ));

    pub fn new(script: PreprocessScript) -> Result<Self, LoadPreprocessRuntimeRhaiError> {
        let mut engine = Engine::new();
        let mut scope = Scope::new();

        register_log_function_rhai!(engine, error);
        register_log_function_rhai!(engine, warn);
        register_log_function_rhai!(engine, info);
        register_log_function_rhai!(engine, debug);
        register_log_function_rhai!(engine, trace);

        engine
            .run_with_scope(&mut scope, Self::PREPROCESS_SCRIPT_PREAMBLE_RHAI)
            .map_err(LoadPreprocessRuntimeRhaiError::Configure)?;

        let ast =
            match script {
                PreprocessScript::File { path, .. } => engine
                    .compile_file_with_scope(&scope, path)
                    .map_err(LoadPreprocessRuntimeRhaiError::Execute)?,
                PreprocessScript::Inline { script, .. } => engine
                    .compile_with_scope(&scope, &script)
                    .map_err(LoadPreprocessRuntimeRhaiError::Parse)?,
            };

        Ok(Self {
            inner: Rc::new(RhaiInner {
                engine: Box::new(engine),
                scope: RefCell::new(scope),
                ast,
            }),
        })
    }
}

impl PreprocessRuntime for PreprocessRhai {
    fn function(
        &self,
        name: &str,
    ) -> Result<Option<Box<dyn PreprocessTransform>>, PreprocessFunctionError> {
        // we cannot obtain a fn handle in rhai like with lua
        if self.inner.ast.iter_functions().any(|f| {
            f.name == name && f.access == FnAccess::Public && (1..=2).contains(&f.params.len())
        }) {
            let transform = PreprocessRhaiTransform {
                inner: self.inner.clone(),
                function: name.to_owned(),
            };

            Ok(Some(Box::new(transform)))
        } else {
            Ok(None)
        }
    }
}

impl PreprocessTransform for PreprocessRhaiTransform {
    fn transform(
        &self,
        record: DataSourceRecord,
    ) -> Result<Option<DataSourceRecord>, PreprocessTransformError> {
        let index = record.index();

        let fields: Map = record
            .into_iter()
            .map(|(k, v)| (k.as_ref().into(), v.into()))
            .collect();

        let RhaiInner { engine, scope, ast } = self.inner.as_ref();

        let result = engine
            .call_fn::<Dynamic>(
                &mut scope.borrow_mut(),
                ast,
                &self.function,
                (fields, index),
            )
            .map_err(PreprocessTransformRhaiError::Execute)?;

        let fields = result
            .try_cast_result::<Map>()
            .map_err(|err| PreprocessTransformRhaiError::ResultNotMap(err.type_name().into()))?;

        if let Some((field, value)) = fields.iter().find(|(_, v)| !v.is_string()) {
            error!(
                "Transform function '{function}' field '{field}' is an unexpected type '{type_name}'",
                function = self.function,
                field = field,
                type_name = value.type_name()
            );

            Err(Box::new(PreprocessTransformRhaiError::FieldNotString {
                field: field.to_string(),
                type_name: value.type_name().into(),
            }))
        } else {
            let fields = fields
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        v.into_immutable_string()
                            .expect("Field values validated to be strings"),
                    )
                })
                .collect();

            Ok(Some(DataSourceRecord::new(fields, index)))
        }
    }
}
