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
}

impl ExtensionRegistry {
    /// Create a new registry and collect all inventory-registered extensions.
    /// Callers that want a truly empty registry (e.g., for testing) can use
    /// `ExtensionRegistry::default()`.
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

    /// Get an input adapter by ID.
    pub fn get_input_adapter(&self, id: &str) -> Option<Box<dyn InputAdapter>> {
        self.input_adapters.get(id).map(|r| (r.create)())
    }

    /// List all registered protocols.
    pub fn list_protocols(&self) -> Vec<String> {
        self.protocols.keys().cloned().collect()
    }

    /// List all registered outputs.
    pub fn list_outputs(&self) -> Vec<String> {
        self.outputs.keys().cloned().collect()
    }

    /// List all registered inputs.
    pub fn list_inputs(&self) -> Vec<String> {
        self.input_adapters.keys().cloned().collect()
    }

    /// List all registered JS modules.
    pub fn list_js_modules(&self) -> Vec<String> {
        self.js_modules.keys().cloned().collect()
    }

    /// Collect all inventory-registered input adapters.
    /// Called once at startup to populate the registry from
    /// `inventory::submit!` calls throughout the codebase.
    ///
    /// Currently handles input adapters. Protocol, output, and other
    /// extension types follow the same pattern when they implement
    /// compile-time registration.
    pub fn collect_inventory(&mut self) {
        tracing::debug!("Collecting inventory-registered input adapters");
        for registration in inventory::iter::<InputAdapterRegistration> {
            self.register_input_adapter(registration.id, InputAdapterRegistration {
                id: registration.id,
                create: registration.create,
            });
        }
        let count = self.input_adapters.len();
        tracing::debug!("Collected {} input adapter(s) from inventory", count);
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

    /// Resolve an input adapter by explicit format ID (e.g. `--format postman`).
    /// Returns `None` if no adapter with that ID is registered.
    pub fn resolve_input_by_id(&self, id: &str) -> Option<Box<dyn InputAdapter>> {
        self.input_adapters.get(id).map(|r| (r.create)())
    }
}
