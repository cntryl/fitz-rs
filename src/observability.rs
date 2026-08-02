use crate::ConnectionState;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Structured, dependency-light telemetry attributes.
pub type FitzAttributes = BTreeMap<String, String>;

pub trait FitzLogger: Send + Sync + 'static {
    fn event(&self, name: &'static str, attributes: &FitzAttributes);
}

pub trait FitzSpan: Send {
    fn set_error(&mut self, message: &str);
    fn end(self: Box<Self>);
}

pub trait FitzTracer: Send + Sync + 'static {
    fn start(&self, name: &'static str, attributes: &FitzAttributes) -> Box<dyn FitzSpan>;
}

pub trait FitzMeter: Send + Sync + 'static {
    fn increment(&self, name: &'static str, value: u64, attributes: &FitzAttributes);
    fn record(&self, name: &'static str, value: f64, attributes: &FitzAttributes);
}

#[derive(Debug, Clone)]
pub struct FitzLifecycleEvent {
    pub name: &'static str,
    pub state: ConnectionState,
    pub attributes: FitzAttributes,
}

pub type LifecycleHandler = Arc<dyn Fn(FitzLifecycleEvent) + Send + Sync>;

#[derive(Clone, Default)]
pub struct FitzObservability {
    pub logger: Option<Arc<dyn FitzLogger>>,
    pub tracer: Option<Arc<dyn FitzTracer>>,
    pub meter: Option<Arc<dyn FitzMeter>>,
    pub lifecycle: Option<LifecycleHandler>,
}
