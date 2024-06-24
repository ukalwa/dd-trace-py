use anyhow::Result;
use pyo3::prelude::*;
use pyo3::types::PyFunction;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task::spawn;
use tokio::task::JoinHandle;

use datadog_remote_config::fetch::ConfigInvariants;
use datadog_remote_config::fetch::FileStorage;
use datadog_remote_config::fetch::SingleFetcher;
use datadog_remote_config::RemoteConfigCapabilities;
use datadog_remote_config::RemoteConfigPath;
use datadog_remote_config::RemoteConfigProduct;
use datadog_remote_config::Target;
use ddcommon::Endpoint;

#[derive(Default)]
struct Storage {
    pub files: Mutex<HashMap<RemoteConfigPath, Arc<Mutex<DataStore>>>>,
}

#[derive(Default, Clone)]
struct RcStorage(Arc<Storage>);

struct PathStore {
    path: RemoteConfigPath,
    storage: Arc<RcStorage>,
    pub data: Arc<Mutex<DataStore>>,
}

#[derive(Debug, Eq, PartialEq)]
struct DataStore {
    pub version: u64,
    pub contents: String,
}

impl Drop for PathStore {
    fn drop(&mut self) {
        self.storage.0.files.lock().unwrap().remove(&self.path);
    }
}

impl FileStorage for RcStorage {
    type StoredFile = PathStore;

    fn store(
        &self,
        version: u64,
        path: RemoteConfigPath,
        contents: Vec<u8>,
    ) -> Result<Arc<Self::StoredFile>> {
        let data = Arc::new(Mutex::new(DataStore {
            version,
            contents: String::from_utf8(contents).unwrap(),
        }));
        assert!(self
            .0
            .files
            .lock()
            .unwrap()
            .insert(path.clone(), data.clone())
            .is_none());
        Ok(Arc::new(PathStore {
            path: path.clone(),
            storage: self.clone().into(),
            data,
        }))
    }

    fn update(&self, file: &Arc<Self::StoredFile>, version: u64, contents: Vec<u8>) -> Result<()> {
        *file.data.lock().unwrap() = DataStore {
            version,
            contents: String::from_utf8(contents).unwrap(),
        };
        Ok(())
    }
}

#[pyclass(name = "RemoteConfigEndpoint", module = "ddtrace.internal.core._core")]
pub struct RemoteConfigEndpointPy {
    url: String,
    api_key: Option<String>,
}

#[pyclass(name = "RemoteConfigProduct", module = "ddtrace.internal.core._core")]
pub enum RemoteConfigProductPy {
    ApmTracing = RemoteConfigProduct::ApmTracing as isize,
    LiveDebugger = RemoteConfigProduct::LiveDebugger as isize,
}

#[pyclass(
    name = "RemoteConfigCapabilitiesPy",
    module = "ddtrace.internal.core._core"
)]
pub enum RemoteConfigCapabilitiesPy {
    AsmActivation = RemoteConfigCapabilities::AsmActivation as isize,
    AsmIpBlocking = RemoteConfigCapabilities::AsmIpBlocking as isize,
    AsmDdRules = RemoteConfigCapabilities::AsmDdRules as isize,
    AsmExclusions = RemoteConfigCapabilities::AsmExclusions as isize,
    AsmRequestBlocking = RemoteConfigCapabilities::AsmRequestBlocking as isize,
    AsmResponseBlocking = RemoteConfigCapabilities::AsmResponseBlocking as isize,
    AsmUserBlocking = RemoteConfigCapabilities::AsmUserBlocking as isize,
    AsmCustomRules = RemoteConfigCapabilities::AsmCustomRules as isize,
    AsmCustomBlockingResponse = RemoteConfigCapabilities::AsmCustomBlockingResponse as isize,
    AsmTrustedIps = RemoteConfigCapabilities::AsmTrustedIps as isize,
    AsmApiSecuritySampleRate = RemoteConfigCapabilities::AsmApiSecuritySampleRate as isize,
    ApmTracingSampleRate = RemoteConfigCapabilities::ApmTracingSampleRate as isize,
    ApmTracingLogsInjection = RemoteConfigCapabilities::ApmTracingLogsInjection as isize,
    ApmTracingHttpHeaderTags = RemoteConfigCapabilities::ApmTracingHttpHeaderTags as isize,
    ApmTracingCustomTags = RemoteConfigCapabilities::ApmTracingCustomTags as isize,
    AsmProcessorOverrides = RemoteConfigCapabilities::AsmProcessorOverrides as isize,
    AsmCustomDataScanners = RemoteConfigCapabilities::AsmCustomDataScanners as isize,
    AsmExclusionData = RemoteConfigCapabilities::AsmExclusionData as isize,
    ApmTracingEnabled = RemoteConfigCapabilities::ApmTracingEnabled as isize,
    ApmTracingDataStreamsEnabled = RemoteConfigCapabilities::ApmTracingDataStreamsEnabled as isize,
    AsmRaspSqli = RemoteConfigCapabilities::AsmRaspSqli as isize,
    AsmRaspLfi = RemoteConfigCapabilities::AsmRaspLfi as isize,
    AsmRaspSsrf = RemoteConfigCapabilities::AsmRaspSsrf as isize,
    AsmRaspShi = RemoteConfigCapabilities::AsmRaspShi as isize,
    AsmRaspXxe = RemoteConfigCapabilities::AsmRaspXxe as isize,
    AsmRaspRce = RemoteConfigCapabilities::AsmRaspRce as isize,
    AsmRaspNosqli = RemoteConfigCapabilities::AsmRaspNosqli as isize,
    AsmRaspXss = RemoteConfigCapabilities::AsmRaspXss as isize,
    ApmTracingSampleRules = RemoteConfigCapabilities::ApmTracingSampleRules as isize,
    CsmActivation = RemoteConfigCapabilities::CsmActivation as isize,
}

impl Into<RemoteConfigCapabilities> for RemoteConfigCapabilitiesPy {
    fn into(self) -> RemoteConfigCapabilities {
        (self as isize).into()
    }
}

impl Into<RemoteConfigProduct> for RemoteConfigProductPy {
    fn into(self) -> RemoteConfigProduct {
        (self as isize).into()
    }
}

#[pyclass(name = "RemoteConfigSettings", module = "ddtrace.internal.core._core")]
pub struct RemoteConfigSettingsPy {
    language: String,
    tracer_version: String,
    endpoint: RemoteConfigEndpointPy,
    products: Vec<RemoteConfigProductPy>,
    capabilities: Vec<RemoteConfigCapabilitiesPy>,
}

#[pyclass(name = "RemoteConfigClient", module = "ddtrace.internal.core._core")]
pub struct RemoteConfigClientPy {
    fetcher: SingleFetcher<RcStorage>,
    on_fetch: Py<PyFunction>,
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
        config_bound: &Bound<'_, RemoteConfigSettingsPy>,
        on_fetch: &Bound<'_, PyFunction>,
    ) -> Self {
        let config = config_bound.borrow();
        let invariants = ConfigInvariants {
            language: config.language,
            tracer_version: config.tracer_version,
            endpoint: Endpoint {
                url: config.endpoint.url,
                api_key: config.endpoint.api_key,
            },
            products: config.products.into_iter().map(|p| p.into()).collect(),
            capabilities: config.capabilities.into_iter().map(|c| c.into()).collect(),
        };

        RemoteConfigClientPy {
            fetcher: SingleFetcher::new(
                RcStorage::default(),
                Target {
                    service,
                    env,
                    app_version,
                },
                runtime_id,
                invariants,
            ),
            on_fetch: on_fetch.unbind(),
            handle: None,
        }
    }

    pub fn start(&self) -> PyResult<()> {
        if self.handle.is_none() {}
        Ok(())
    }

    pub fn stop(&self) -> PyResult<()> {
        Ok(())
    }
}
