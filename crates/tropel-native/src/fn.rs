use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct ExtraFunctionsModule;

impl NativeModule for ExtraFunctionsModule {
    fn name(&self) -> &str {
        "__tropel_native_fn"
    }

    fn install(&self, ctx: &mut JsContext) -> Result<()> {
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
///
/// An empty or inverted range (`min >= max`) returns `min` instead of
/// panicking — this unwinds out of a QuickJS callback when called from a
/// script, so it must never panic (backlog P3).
pub fn random_int(min: i64, max: i64) -> i64 {
    use rand::RngExt;
    if min >= max {
        return min;
    }
    let mut rng = rand::rng();
    rng.random_range(min..max)
}

/// Generate a random float in [0, 1).
pub fn random_float() -> f64 {
    use rand::RngExt;
    let mut rng = rand::rng();
    rng.random::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_int_empty_range_does_not_panic() {
        // Backlog P3: `random_range(min..max)` panics on an empty/inverted
        // range (unwinding out of a QuickJS callback). Must return `min`.
        assert_eq!(random_int(5, 5), 5);
        assert_eq!(random_int(7, 3), 7);
        // Valid ranges still produce in-range values.
        for _ in 0..100 {
            let v = random_int(0, 10);
            assert!((0..10).contains(&v));
        }
    }
}
