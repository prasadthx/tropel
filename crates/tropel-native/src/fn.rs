use crate::NativeModule;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct ExtraFunctionsModule;

impl NativeModule for ExtraFunctionsModule {
    fn name(&self) -> &str {
        "__tropel_native_fn"
    }

    fn install(&self, _ctx: &JsContext) -> Result<()> {
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
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen_range(min..max)
}

/// Generate a random float in [0, 1).
pub fn random_float() -> f64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    rng.gen::<f64>()
}
