use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct HashModule;

impl NativeModule for HashModule {
    fn name(&self) -> &str {
        "__tropel_native_hash"
    }

    fn install(&self, ctx: &mut JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();
            // Hash module reuses crypto functions — add any hash-specific bindings here
            let _ = globals.set(
                "__tropel_native_hash_uuid",
                Func::from(|| -> String { uuid::Uuid::new_v4().to_string() }),
            );
        });

        tracing::debug!("Installed hash native module");
        Ok(())
    }
}
