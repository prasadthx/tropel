use crate::traits::*;
use std::collections::HashMap;
use std::sync::Arc;

/// The extension registry: collects all registered extensions at startup.
#[derive(Clone, Default)]
pub struct ExtensionRegistry {
    protocols: HashMap<String, Arc<ProtocolRegistration>>,
    outputs: HashMap<String, Arc<OutputRegistration>>,
    js_modules: HashMap<String, Arc<JsModuleRegistration>>,
    auth_signers: HashMap<String, Arc<AuthSignerRegistration>>,
    input_adapters: HashMap<String, Arc<InputAdapterRegistration>>,
    // Runtime-constructed adapter instances (e.g. subprocess adapters).
    // These bypass the fn-pointer restriction of InputAdapterRegistration.
    // Runtime factory functions for adapters that need configuration at startup.
    // Unlike InputAdapterRegistration (fn() pointer, for inventory::submit!),
    // these closures can capture runtime values (e.g. a subprocess command).
    input_adapter_factories: HashMap<String, Arc<dyn Fn() -> Box<dyn InputAdapter> + Send + Sync>>,
    drivers: HashMap<String, Arc<DriverRegistration>>,
}

impl ExtensionRegistry {
    /// Create a new registry and collect all inventory-registered extensions.
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.collect_inventory();
        registry
    }

    /// Register a protocol.
    pub fn register_protocol(&mut self, scheme: &str, registration: ProtocolRegistration) {
        self.protocols.insert(scheme.to_string(), Arc::new(registration));
    }

    /// Register an output.
    pub fn register_output(&mut self, name: &str, registration: OutputRegistration) {
        self.outputs.insert(name.to_string(), Arc::new(registration));
    }

    /// Register a JS module.
    pub fn register_js_module(&mut self, specifier: &str, registration: JsModuleRegistration) {
        self.js_modules.insert(specifier.to_string(), Arc::new(registration));
    }

    /// Register an auth signer.
    pub fn register_auth_signer(&mut self, kind: &str, registration: AuthSignerRegistration) {
        self.auth_signers.insert(kind.to_string(), Arc::new(registration));
    }

    /// Register an input adapter.
    pub fn register_input_adapter(&mut self, id: &str, registration: InputAdapterRegistration) {
        self.input_adapters.insert(id.to_string(), Arc::new(registration));
    }

    /// Register a driver.
    pub fn register_driver(&mut self, id: &str, registration: DriverRegistration) {
        self.drivers.insert(id.to_string(), Arc::new(registration));
    }

    /// Get a protocol by scheme.
    pub fn get_protocol(&self, scheme: &str) -> Option<Box<dyn Protocol>> {
        self.protocols.get(scheme).map(|r| (r.factory)())
    }

    /// Get an output by name.
    pub fn get_output(&self, name: &str) -> Option<Box<dyn Output>> {
        self.outputs.get(name).map(|r| (r.factory)())
    }

    /// Get a JS module by specifier.
    pub fn get_js_module(&self, specifier: &str) -> Option<Box<dyn JsModule>> {
        self.js_modules.get(specifier).map(|r| (r.factory)())
    }

    /// Get an auth signer by kind.
    pub fn get_auth_signer(&self, kind: &str) -> Option<Box<dyn AuthSigner>> {
        self.auth_signers.get(kind).map(|r| (r.factory)())
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

    /// List all registered JS modules.
    pub fn list_js_modules(&self) -> Vec<String> {
        self.js_modules.keys().cloned().collect()
    }

    /// Collect all inventory-registered extensions at startup.
    /// Populates input adapters and drivers from `inventory::submit!` calls.
    pub fn collect_inventory(&mut self) {
        tracing::debug!("Collecting inventory-registered input adapters");
        for registration in inventory::iter::<InputAdapterRegistration> {
            self.register_input_adapter(registration.id, InputAdapterRegistration {
                id: registration.id,
                create: registration.create,
            });
        }
        let adapter_count = self.input_adapters.len();
        tracing::debug!("Collected {} input adapter(s) from inventory", adapter_count);

        tracing::debug!("Collecting inventory-registered drivers");
        for registration in inventory::iter::<DriverRegistration> {
            self.register_driver(registration.id, DriverRegistration {
                id: registration.id,
                create: registration.create,
            });
        }
        let driver_count = self.drivers.len();
        tracing::debug!("Collected {} driver(s) from inventory", driver_count);
    }

    /// Resolve an input adapter from raw bytes using content detection.
    /// Iterates all registered adapters in registration order and returns
    /// the first one whose `detect()` returns `true`. Returns `None` if
    /// no adapter claims the bytes.
    pub fn resolve_input(&self, bytes: &[u8]) -> Option<Box<dyn InputAdapter>> {
        for registration in self.input_adapters.values() {
            let adapter = (registration.create)();
            if adapter.detect(bytes) {
                return Some(adapter);
            }
        }
        None
    }

    /// Resolve a driver from raw bytes using content detection.
    /// Iterates all registered drivers and returns the first one whose
    /// `detect()` returns `true`.
    pub fn resolve_driver(&self, bytes: &[u8]) -> Option<Box<dyn Driver>> {
        for registration in self.drivers.values() {
            let driver = (registration.create)();
            if driver.detect(bytes) {
                return Some(driver);
            }
        }
        // Also check factory adapters? No — drivers use a separate path.
        None
    }

    /// Resolve a driver by explicit ID.
    pub fn resolve_driver_by_id(&self, id: &str) -> Option<Box<dyn Driver>> {
        self.drivers.get(id).map(|r| (r.create)())
    }
}
