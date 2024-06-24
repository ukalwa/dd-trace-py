mod rate_limiter;
mod remote_config;

use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<rate_limiter::RateLimiterPy>()?;
    m.add_class::<remote_config::RemoteConfigClientPy>()?;
    Ok(())
}
