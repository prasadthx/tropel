use crate::traits::*;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;

/// The extension registry: collects all registered extensions at startup.
///
/// All maps are `IndexMap` (insertion-ordered) so content auto-detection
/// (`resolve_input` / `resolve_driver`) is deterministic: among adapters
/// whose `detect()` claims a document, the highest-`priority` wins (ties
/// fall back to insertion order) — not a random `HashMap` iteration winner.
/// Built-ins register via `inventory` and declare explicit priorities with
/// `with_priority`; runtime factories are appended after the inventory pass
/// and act as a fallback (priority 0).
#[derive(Clone, Default)]
pub struct ExtensionRegistry {
    protocols: IndexMap<String, Arc<ProtocolRegistration>>,
    outputs: IndexMap<String, Arc<OutputRegistration>>,
    input_adapters: IndexMap<String, Arc<InputAdapterRegistration>>,
    // Runtime-constructed adapter instances (e.g. subprocess adapters).
    // These bypass the fn-pointer restriction of InputAdapterRegistration.
    // Runtime factory functions for adapters that need configuration at startup.
    // Unlike InputAdapterRegistration (fn() pointer, for inventory::submit!),
    // these closures can capture runtime values (e.g. a subprocess command).
    input_adapter_factories: IndexMap<String, Arc<dyn Fn() -> Box<dyn InputAdapter> + Send + Sync>>,
    drivers: IndexMap<String, Arc<DriverRegistration>>,
}

impl ExtensionRegistry {
    /// Create a new registry and collect all inventory-registered extensions.
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.collect_inventory();
        registry
    }

    /// Register a protocol.
    pub fn register_protocol(&mut self, registration: ProtocolRegistration) {
        self.protocols
            .insert(registration.scheme.to_string(), Arc::new(registration));
    }

    /// Register an output.
    pub fn register_output(&mut self, name: &str, registration: OutputRegistration) {
        self.outputs
            .insert(name.to_string(), Arc::new(registration));
    }

    /// Register an input adapter.
    pub fn register_input_adapter(&mut self, id: &str, registration: InputAdapterRegistration) {
        self.input_adapters
            .insert(id.to_string(), Arc::new(registration));
    }

    /// Register a driver.
    pub fn register_driver(&mut self, id: &str, registration: DriverRegistration) {
        self.drivers.insert(id.to_string(), Arc::new(registration));
    }

    /// Get a protocol by scheme.
    pub fn get_protocol(&self, scheme: &str) -> Option<Box<dyn Protocol>> {
        self.protocols.get(scheme).map(|r| (r.create)())
    }

    /// Instantiate EVERY registered protocol into a scheme-keyed map.
    ///
    /// The engine passes this map to the runner once per scenario so any
    /// registered protocol (gRPC, WebSocket, or a third-party one) is
    /// dispatched by its URL scheme — not just hardcoded `grpc`/`ws` slots.
    pub fn instantiate_protocols(&self) -> HashMap<String, Arc<dyn Protocol>> {
        self.protocols
            .iter()
            .map(|(scheme, reg)| (scheme.clone(), Arc::from((reg.create)())))
            .collect()
    }

    /// Get an output by name.
    pub fn get_output(&self, name: &str) -> Option<Box<dyn Output>> {
        self.outputs.get(name).map(|r| (r.create)())
    }

    /// Register an adapter factory closure.
    ///
    /// Unlike `register_input_adapter()` (which takes a `fn()` pointer for
    /// compile-time `inventory::submit!` compat), this accepts an `Arc<dyn Fn>`
    /// closure that can capture runtime values. Use this for adapters that
    /// need runtime configuration (e.g. subprocess adapter command string).
    pub fn register_adapter_factory(
        &mut self,
        id: &str,
        factory: Arc<dyn Fn() -> Box<dyn InputAdapter> + Send + Sync>,
    ) {
        self.input_adapter_factories.insert(id.to_string(), factory);
    }

    /// Get an input adapter by ID — checks factory closures first, then registrations.
    pub fn get_input_adapter(&self, id: &str) -> Option<Box<dyn InputAdapter>> {
        if let Some(factory) = self.input_adapter_factories.get(id) {
            return Some((factory)());
        }
        self.input_adapters.get(id).map(|r| (r.create)())
    }

    /// Resolve an input adapter by explicit format ID.
    pub fn resolve_input_by_id(&self, id: &str) -> Option<Box<dyn InputAdapter>> {
        self.get_input_adapter(id)
    }

    /// List all registered inputs.
    pub fn list_inputs(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.input_adapters.keys().cloned().collect();
        ids.extend(self.input_adapter_factories.keys().cloned());
        ids.sort();
        ids.dedup();
        ids
    }

    /// Get a driver by ID.
    pub fn get_driver(&self, id: &str) -> Option<Box<dyn Driver>> {
        self.drivers.get(id).map(|r| (r.create)())
    }

    /// List all registered protocols.
    pub fn list_protocols(&self) -> Vec<String> {
        self.protocols.keys().cloned().collect()
    }

    /// List all registered outputs.
    pub fn list_outputs(&self) -> Vec<String> {
        self.outputs.keys().cloned().collect()
    }

    /// List all registered drivers.
    pub fn list_drivers(&self) -> Vec<String> {
        self.drivers.keys().cloned().collect()
    }

    /// Collect all inventory-registered extensions at startup.
    /// Populates input adapters and drivers from `inventory::submit!` calls.
    pub fn collect_inventory(&mut self) {
        tracing::debug!("Collecting inventory-registered input adapters");
        for registration in inventory::iter::<InputAdapterRegistration> {
            self.register_input_adapter(
                registration.id,
                InputAdapterRegistration {
                    id: registration.id,
                    create: registration.create,
                    priority: registration.priority,
                },
            );
        }
        let adapter_count = self.input_adapters.len();
        tracing::debug!(
            "Collected {} input adapter(s) from inventory",
            adapter_count
        );

        tracing::debug!("Collecting inventory-registered drivers");
        for registration in inventory::iter::<DriverRegistration> {
            self.register_driver(
                registration.id,
                DriverRegistration {
                    id: registration.id,
                    create: registration.create,
                    priority: registration.priority,
                },
            );
        }
        let driver_count = self.drivers.len();
        tracing::debug!("Collected {} driver(s) from inventory", driver_count);

        tracing::debug!("Collecting inventory-registered outputs");
        for registration in inventory::iter::<OutputRegistration> {
            self.register_output(
                registration.id,
                OutputRegistration {
                    id: registration.id,
                    create: registration.create,
                    priority: registration.priority,
                },
            );
        }
        let output_count = self.outputs.len();
        tracing::debug!("Collected {} output(s) from inventory", output_count);

        tracing::debug!("Collecting inventory-registered protocols");
        for registration in inventory::iter::<ProtocolRegistration> {
            self.register_protocol(ProtocolRegistration {
                scheme: registration.scheme,
                create: registration.create,
                priority: registration.priority,
            });
        }
        let protocol_count = self.protocols.len();
        tracing::debug!("Collected {} protocol(s) from inventory", protocol_count);
    }

    /// Resolve an input adapter from raw bytes using content detection.
    /// Iterates all registered adapters in registration order and returns
    /// the first one whose `detect()` returns `true`. Returns `None` if
    /// no adapter claims the bytes.
    ///
    /// Also probes factory-registered adapters (e.g. WASM plugins loaded from
    /// `--plugins-dir`), so content auto-detection works for runtime plugins
    /// too, not just compile-time `inventory` registrations.
    ///
    /// Dispatch is **explicit-priority-first**: among all adapters whose
    /// `detect()` claims the bytes, the one with the highest `priority` wins
    /// (ties fall back to registration order — stable IndexMap iteration).
    /// This removes the dependency on `inventory` link order.
    pub fn resolve_input(&self, bytes: &[u8]) -> Option<Box<dyn InputAdapter>> {
        let mut best: Option<(u8, Box<dyn InputAdapter>)> = None;
        for registration in self.input_adapters.values() {
            let adapter = (registration.create)();
            // Strictly-greater: on equal priority the FIRST registration wins
            // (ties → registration order), and inventory adapters beat
            // equal-priority factory adapters (factories are probed after).
            if adapter.detect(bytes)
                && best
                    .as_ref()
                    .map(|(p, _)| registration.priority > *p)
                    .unwrap_or(true)
            {
                best = Some((registration.priority, adapter));
            }
        }
        for factory in self.input_adapter_factories.values() {
            let adapter = (factory)();
            if adapter.detect(bytes) {
                let p = 0;
                if best.as_ref().map(|(bp, _)| p > *bp).unwrap_or(true) {
                    best = Some((p, adapter));
                }
            }
        }
        best.map(|(_, adapter)| adapter)
    }

    /// Resolve a driver from raw bytes using content detection.
    /// Highest-priority driver whose `detect()` returns `true` wins (ties
    /// fall back to registration order).
    pub fn resolve_driver(&self, bytes: &[u8]) -> Option<Box<dyn Driver>> {
        let mut best: Option<(u8, Box<dyn Driver>)> = None;
        for registration in self.drivers.values() {
            let driver = (registration.create)();
            // Strictly-greater: first registration wins on equal priority.
            if driver.detect(bytes)
                && best
                    .as_ref()
                    .map(|(p, _)| registration.priority > *p)
                    .unwrap_or(true)
            {
                best = Some((registration.priority, driver));
            }
        }
        best.map(|(_, driver)| driver)
    }

    /// Resolve a driver by explicit ID.
    pub fn resolve_driver_by_id(&self, id: &str) -> Option<Box<dyn Driver>> {
        self.drivers.get(id).map(|r| (r.create)())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::Value;
    use tropel_core::scenario::Scenario;
    use tropel_core::types::{Request, Sample};
    use tropel_core::TropelError;

    // ── Stubs (pure-content detection, no real parsing) ──
    struct StubAdapter {
        id: &'static str,
        detect_prefix: &'static [u8],
    }
    impl InputAdapter for StubAdapter {
        fn id(&self) -> &str {
            self.id
        }
        fn detect(&self, bytes: &[u8]) -> bool {
            bytes.starts_with(self.detect_prefix)
        }
        fn parse(&self, _bytes: &[u8]) -> tropel_core::Result<Scenario> {
            Err(TropelError::Other("stub adapter parse not implemented".into()))
        }
    }

    struct StubDriver {
        id: &'static str,
    }
    #[async_trait]
    impl Driver for StubDriver {
        fn id(&self) -> &str {
            self.id
        }
        fn detect(&self, bytes: &[u8]) -> bool {
            bytes.starts_with(b"js:")
        }
        async fn init(
            &self,
            _bytes: &[u8],
            _source_path: Option<&std::path::Path>,
            _exec: Option<&str>,
        ) -> tropel_core::Result<Box<dyn DriverInstance>> {
            Err(TropelError::Other("stub driver init not implemented".into()))
        }
    }

    struct StubProtocol {
        scheme: &'static str,
    }
    #[async_trait]
    impl Protocol for StubProtocol {
        fn scheme(&self) -> &str {
            self.scheme
        }
        async fn execute(
            &self,
            _req: &Request,
            _config: Option<&Value>,
        ) -> tropel_core::Result<ProtocolOutcome> {
            Err(TropelError::Other("stub protocol execute not implemented".into()))
        }
    }

    struct StubOutput {
        id: &'static str,
    }
    #[async_trait]
    impl Output for StubOutput {
        fn name(&self) -> &str {
            self.id
        }
        async fn emit(&self, _batch: &[Sample]) -> tropel_core::Result<()> {
            Ok(())
        }
        async fn flush(&self) -> tropel_core::Result<()> {
            Ok(())
        }
    }

    fn stub_protocol() -> Box<dyn Protocol> {
        Box::new(StubProtocol { scheme: "grpc" })
    }
    fn stub_output() -> Box<dyn Output> {
        Box::new(StubOutput { id: "stdout" })
    }
    fn stub_adapter() -> Box<dyn InputAdapter> {
        Box::new(StubAdapter {
            id: "stub",
            detect_prefix: b"{}",
        })
    }
    fn stub_driver() -> Box<dyn Driver> {
        Box::new(StubDriver { id: "k6" })
    }

    #[test]
    fn register_and_get_by_id_all_four_kinds() {
        let mut reg = ExtensionRegistry::default();
        reg.register_protocol(ProtocolRegistration::new("grpc", stub_protocol));
        reg.register_output("stdout", OutputRegistration::new("stdout", stub_output));
        reg.register_input_adapter("postman", InputAdapterRegistration::new("postman", stub_adapter));
        reg.register_driver("k6", DriverRegistration::new("k6", stub_driver));

        assert!(reg.get_protocol("grpc").is_some());
        assert!(reg.get_protocol("nope").is_none());
        assert!(reg.get_output("stdout").is_some());
        assert!(reg.get_input_adapter("postman").is_some());
        assert!(reg.resolve_input_by_id("postman").is_some());
        assert!(reg.get_driver("k6").is_some());
        assert!(reg.resolve_driver_by_id("k6").is_some());
        assert!(reg.get_driver("nope").is_none());
    }

    #[test]
    fn factory_registration_takes_precedence_over_adapter_registration() {
        let mut reg = ExtensionRegistry::default();
        reg.register_input_adapter("dup", InputAdapterRegistration::new("dup", stub_adapter));
        reg.register_adapter_factory("dup", Arc::new(|| -> Box<dyn InputAdapter> {
            Box::new(StubAdapter {
                id: "factory-wins",
                detect_prefix: b"F",
            })
        }));
        // Factories are checked first (runtime config overrides static).
        let got = reg.get_input_adapter("dup").unwrap();
        assert_eq!(got.id(), "factory-wins");
    }

    #[test]
    fn resolve_input_claims_by_content_prefix() {
        let mut reg = ExtensionRegistry::default();
        reg.register_input_adapter("json", InputAdapterRegistration::new("json", || {
            Box::new(StubAdapter {
                id: "json",
                detect_prefix: b"{",
            })
        }));
        reg.register_input_adapter("xml", InputAdapterRegistration::new("xml", || {
            Box::new(StubAdapter {
                id: "xml",
                detect_prefix: b"<",
            })
        }));

        assert_eq!(reg.resolve_input(b"{...}").unwrap().id(), "json");
        assert_eq!(reg.resolve_input(b"<x/>").unwrap().id(), "xml");
        // No adapter claims these bytes.
        assert!(reg.resolve_input(b"plain text").is_none());
        // Explicit ID resolution ignores detection.
        assert_eq!(reg.resolve_input_by_id("xml").unwrap().id(), "xml");
    }

    #[test]
    fn resolve_input_priority_wins_over_registration_order() {
        let mut reg = ExtensionRegistry::default();
        // Both detect `b"{` — the LOWER-priority one is registered first.
        reg.register_input_adapter("generic", InputAdapterRegistration::new("generic", || {
            Box::new(StubAdapter {
                id: "generic",
                detect_prefix: b"{",
            })
        }));
        reg.register_input_adapter(
            "specific",
            InputAdapterRegistration::new("specific", || {
                Box::new(StubAdapter {
                    id: "specific",
                    detect_prefix: b"{",
                })
            })
            .with_priority(10),
        );
        // Highest priority wins, not first-registered.
        assert_eq!(reg.resolve_input(b"{...}").unwrap().id(), "specific");
    }

    #[test]
    fn resolve_driver_uses_content_detection() {
        let mut reg = ExtensionRegistry::default();
        reg.register_driver("k6", DriverRegistration::new("k6", stub_driver));
        assert!(reg.resolve_driver(b"js:export default...").is_some());
        assert!(reg.resolve_driver(b"nope").is_none());
    }

    #[test]
    fn list_inputs_merges_factories_and_dedups() {
        let mut reg = ExtensionRegistry::default();
        reg.register_input_adapter("b", InputAdapterRegistration::new("b", stub_adapter));
        reg.register_adapter_factory("a", Arc::new(stub_adapter));
        reg.register_adapter_factory("b", Arc::new(stub_adapter)); // duplicate
        let mut ids = reg.list_inputs();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn instantiate_protocols_returns_scheme_keyed_map() {
        let mut reg = ExtensionRegistry::default();
        reg.register_protocol(ProtocolRegistration::new("grpc", stub_protocol));
        reg.register_protocol(ProtocolRegistration::new("ws", || {
            Box::new(StubProtocol { scheme: "ws" })
        }));
        let map = reg.instantiate_protocols();
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("grpc"));
        assert!(map.contains_key("ws"));
        assert_eq!(map["ws"].scheme(), "ws");
    }
}
