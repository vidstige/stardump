/// Parallax quality: SNR = π / max(σ_π, FLOOR)
///
/// The 0.010 mas floor reflects Gaia DR3's global parallax zero-point
/// systematic (~0.017 mas, Lindegren et al. 2021) and calibration noise
/// for bright stars. Actual reported errors already dominate for faint
/// stars, so no magnitude-dependent floor is needed.
///
/// A threshold of 5 means σ_d/d < 20%, acceptable for 3D visualization.

pub const PARALLAX_SYSTEMATIC_FLOOR_MAS: f32 = 0.010;

pub const DEFAULT_PARALLAX_QUALITY_THRESHOLD: f32 = 5.0;

pub fn parallax_quality(parallax_mas: f32, parallax_error_mas: f32) -> Option<f32> {
    if !parallax_mas.is_finite() || parallax_mas <= 0.0 {
        return None;
    }
    if !parallax_error_mas.is_finite() || parallax_error_mas <= 0.0 {
        return None;
    }
    Some(parallax_mas / parallax_error_mas.max(PARALLAX_SYSTEMATIC_FLOOR_MAS))
}

pub fn passes_parallax_quality(
    parallax_mas: f32,
    parallax_error_mas: f32,
    minimum_quality: f32,
) -> bool {
    parallax_quality(parallax_mas, parallax_error_mas)
        .is_some_and(|quality| quality >= minimum_quality)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(left: f32, right: f32) {
        assert!((left - right).abs() < 1e-6, "{left} != {right}");
    }

    #[test]
    fn bright_star_floor_kicks_in() {
        // σ_π = 0.001 mas reported, but floor is 0.010 → quality = 1.0/0.010 = 100
        assert_close(parallax_quality(1.0, 0.001).unwrap(), 100.0);
    }

    #[test]
    fn faint_star_uses_actual_error() {
        // σ_π = 0.2 mas reported, well above floor → quality = 1.0/0.2 = 5.0
        assert_close(parallax_quality(1.0, 0.2).unwrap(), 5.0);
    }

}
