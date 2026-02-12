#[macro_export]
macro_rules! trace_sql {
    ($sql:expr) => {
        trace!("\n{}", $sql.trim());
    };
}
