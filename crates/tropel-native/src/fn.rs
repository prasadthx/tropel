use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct ExtraFunctionsModule;

impl NativeModule for ExtraFunctionsModule {
    fn name(&self) -> &str {
        "__tropel_native_fn"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            let _ = globals.set(
                "__tropel_native_random_int",
                Func::from(|min: i64, max: i64| -> i64 { random_int(min, max) }),
            );

            let _ = globals.set(
                "__tropel_native_random_float",
                Func::from(|| -> f64 { random_float() }),
            );
        });

        tracing::debug!("Installed extra functions native module");
        Ok(())
    }
}

/// Generate a random UUID v4 string.
pub fn generate_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generate a random integer in [min, max).
pub fn random_int(min: i64, max: i64) -> i64 {
    use rand::RngExt;
    let mut rng = rand::rng();
    rng.random_range(min..max)
}

/// Generate a random float in [0, 1).
pub fn random_float() -> f64 {
    use rand::RngExt;
    let mut rng = rand::rng();
    rng.random::<f64>()
}
