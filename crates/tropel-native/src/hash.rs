use crate::NativeModule;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct HashModule;

impl NativeModule for HashModule {
    fn name(&self) -> &str {
        "__tropel_native_hash"
    }

    fn install(&self, _ctx: &JsContext) -> Result<()> {
        tracing::debug!("Installed hash native module");
        Ok(())
    }
}
