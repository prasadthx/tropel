use crate::traits::*;
use std::collections::HashMap;
use std::sync::Arc;

/// The extension registry: collects all registered extensions at startup.
#[derive(Default)]
pub struct ExtensionRegistry {
    protocols: HashMap<String, Arc<ProtocolRegistration>>,
    outputs: HashMap<String, Arc<OutputRegistration>>,
    js_modules: HashMap<String, Arc<JsModuleRegistration>>,
    auth_signers: HashMap<String, Arc<AuthSignerRegistration>>,
    input_adapters: HashMap<String, Arc<InputAdapterRegistration>>,
}

impl ExtensionRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
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
        self.input_adapters.get(id).map(|r| (r.factory)())
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

    /// Collect all inventory-registered extensions.
    /// This is called once at startup to populate the registry from
    /// `inventory::submit!` calls throughout the codebase.
    pub fn collect_inventory(&mut self) {
        // Collect protocols
        // inventory::iter::<ProtocolRegistration>.for_each(|r| {
        //     let scheme = (r.factory)();
        //     self.register_protocol(scheme.scheme(), ...);
        // });
        //
        // Note: inventory integration requires careful per-crate setup.
        // For now, extensions register explicitly or via build configuration.
        tracing::debug!("Collecting inventory-registered extensions");
    }
}
