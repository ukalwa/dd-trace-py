use anyhow::Error;
use http::uri::Uri;
use pyo3::prelude::*;
use pyo3::types::PyFunction;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::Duration;

use datadog_remote_config::fetch::ConfigInvariants;
use datadog_remote_config::fetch::SingleChangesFetcher;
use datadog_remote_config::file_change_tracker::Change;
use datadog_remote_config::file_change_tracker::FilePath;
use datadog_remote_config::file_storage::ParsedFileStorage;
use datadog_remote_config::file_storage::RawFile;
use datadog_remote_config::RemoteConfigData;
use datadog_remote_config::RemoteConfigProduct;
use datadog_remote_config::RemoteConfigSource;
use datadog_remote_config::Target;
use ddcommon::Endpoint;

async fn poll_remote_config(
    service: String,
    env: String,
    app_version: String,
    runtime_id: String,
    on_change: impl Fn(u64, Arc<RawFile<Result<RemoteConfigData, Error>>>),
) {
    let mut fetcher = SingleChangesFetcher::new(
        ParsedFileStorage::default(),
        Target {
            service,
            env,
            app_version,
        },
        runtime_id,
        ConfigInvariants {
            language: "python".to_string(),
            tracer_version: "2.10.0".to_string(),
            endpoint: Endpoint {
                url: "http://localhost:8126".parse::<Uri>().unwrap(),
                api_key: None,
            },
            products: vec![
                RemoteConfigProduct::ApmTracing,
                RemoteConfigProduct::LiveDebugger,
            ],
            capabilities: vec![],
        },
    );

    loop {
        match fetcher.fetch_changes().await {
            Ok(changes) => {
                for change in changes {
                    match change {
                        Change::Add(file) => {
                            on_change(1, file.clone());
                        }
                        Change::Update(file, _) => {
                            on_change(2, file.clone());
                        }
                        Change::Remove(file) => {
                            on_change(3, file.clone());
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Fetch failed with {e}");
                fetcher.set_last_error(e.to_string());
            }
        }

        sleep(Duration::from_nanos(fetcher.get_interval()).max(Duration::from_secs(1))).await;
    }
}

#[pyclass(name = "RemoteConfigPath", module = "ddtrace.internal.core._core")]
pub struct RemoteConfigPathPy {
    pub source: RemoteConfigSource,
    pub product: RemoteConfigProduct,
    pub config_id: String,
    pub name: String,
}

#[pymethods]
impl RemoteConfigPathPy {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "RemoteConfigPath(source=, product={}, config_id={}, name={})",
            self.product, self.config_id, self.name,
        ))
    }
}

#[pyclass(name = "RemoteConfigClient", module = "ddtrace.internal.core._core")]
pub struct RemoteConfigClientPy {
    service: String,
    env: String,
    app_version: String,
    runtime_id: String,
    on_change: Py<PyFunction>,
    rt: Option<Runtime>,
    handle: Option<JoinHandle<()>>,
}

#[pymethods]
impl RemoteConfigClientPy {
    #[new]
    fn new(
        service: String,
        env: String,
        app_version: String,
        runtime_id: String,
        on_change: &Bound<'_, PyFunction>,
    ) -> Self {
        RemoteConfigClientPy {
            service,
            env,
            app_version,
            runtime_id,
            on_change: on_change.clone().unbind(),
            rt: None,
            handle: None,
        }
    }

    fn is_running(&self) -> bool {
        if let Some(handle) = &self.handle {
            return !handle.is_finished();
        }
        false
    }

    pub fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.is_running() {
            return Ok(());
        }

        py.allow_threads(|| {
            if self.rt.is_none() {
                self.rt = Some(Runtime::new().unwrap());
            }

            if let Some(rt) = &self.rt {
                let service = self.service.clone();
                let env = self.env.clone();
                let app_version = self.app_version.clone();
                let runtime_id = self.runtime_id.clone();
                let on_change = self.on_change.clone();

                self.handle = Some(rt.spawn(poll_remote_config(
                    service,
                    env,
                    app_version,
                    runtime_id,
                    move |change_type, file| {
                        let path = RemoteConfigPathPy {
                            source: file.path().source.clone(),
                            product: file.path().product,
                            config_id: file.path().config_id.clone(),
                            name: file.path().name.clone(),
                        };
                        let version = file.version();
                        let contents = match *file.contents() {
                            Ok(data) => Some(data),
                            Err(_) => None,
                        };

                        let _ = Python::with_gil(|py| {
                            on_change
                                .call1(py, (change_type, path, version, format!("{:?}", contents)))
                                .unwrap();
                        });
                    },
                )));
            }
        });
        Ok(())
    }

    pub fn stop(&mut self) -> PyResult<()> {
        if !self.is_running() {
            return Ok(());
        }
        if let Some(handle) = &self.handle {
            handle.abort();
        }
        self.handle = None;
        Ok(())
    }
}
