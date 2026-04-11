/// Gaia DR3 summary reports median parallax uncertainties of roughly:
/// - G < 15: 0.02-0.03 mas
/// - G = 17: 0.07 mas
/// - G = 20: 0.5 mas
/// - G = 21: 1.3 mas
///
/// We interpolate log10(sigma_pi) piecewise between those anchor points to get a
/// simple brightness-based reference floor for parallax uncertainty.
///
/// Sources:
/// - https://www.cosmos.esa.int/web/gaia/dr3
/// - https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html

const BRIGHT_LIMIT_G_MAG: f32 = 15.0;
const FAINT_LIMIT_G_MAG: f32 = 21.0;
const SIGMA_G15_MAS: f32 = 0.025;
const SIGMA_G17_MAS: f32 = 0.070;
const SIGMA_G20_MAS: f32 = 0.500;
const SIGMA_G21_MAS: f32 = 1.300;

pub const DEFAULT_PARALLAX_QUALITY_THRESHOLD: f32 = 10.0;

fn interpolate_log10_sigma(g_mag: f32, g0: f32, sigma0: f32, g1: f32, sigma1: f32) -> f32 {
    let slope = (sigma1.log10() - sigma0.log10()) / (g1 - g0);
    10_f32.powf(sigma0.log10() + slope * (g_mag - g0))
}

pub fn reference_parallax_uncertainty_mas(g_mag: f32) -> Option<f32> {
    if !g_mag.is_finite() {
        return None;
    }

    Some(if g_mag <= BRIGHT_LIMIT_G_MAG {
        SIGMA_G15_MAS
    } else if g_mag <= 17.0 {
        interpolate_log10_sigma(g_mag, 15.0, SIGMA_G15_MAS, 17.0, SIGMA_G17_MAS)
    } else if g_mag <= 20.0 {
        interpolate_log10_sigma(g_mag, 17.0, SIGMA_G17_MAS, 20.0, SIGMA_G20_MAS)
    } else if g_mag <= FAINT_LIMIT_G_MAG {
        interpolate_log10_sigma(g_mag, 20.0, SIGMA_G20_MAS, 21.0, SIGMA_G21_MAS)
    } else {
        SIGMA_G21_MAS
    })
}

pub fn effective_parallax_uncertainty_mas(parallax_error_mas: f32, g_mag: f32) -> Option<f32> {
    if !parallax_error_mas.is_finite() || parallax_error_mas <= 0.0 {
        return None;
    }
    Some(
        reference_parallax_uncertainty_mas(g_mag)
            .map_or(parallax_error_mas, |reference| parallax_error_mas.max(reference)),
    )
}

pub fn parallax_quality(parallax_mas: f32, parallax_error_mas: f32, g_mag: f32) -> Option<f32> {
    if !parallax_mas.is_finite() || parallax_mas <= 0.0 {
        return None;
    }
    Some(parallax_mas / effective_parallax_uncertainty_mas(parallax_error_mas, g_mag)?)
}

pub fn passes_parallax_quality(
    parallax_mas: f32,
    parallax_error_mas: f32,
    g_mag: f32,
    minimum_quality: f32,
) -> bool {
    parallax_quality(parallax_mas, parallax_error_mas, g_mag)
        .is_some_and(|quality| quality >= minimum_quality)
}

pub fn maximum_distance_pc_for_quality(minimum_quality: f32) -> f32 {
    1_000.0 / (minimum_quality * SIGMA_G15_MAS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(left: f32, right: f32) {
        assert!((left - right).abs() < 1e-6, "{left} != {right}");
    }

    #[test]
    fn matches_reference_anchor_points() {
        assert_close(reference_parallax_uncertainty_mas(15.0).unwrap(), 0.025);
        assert_close(reference_parallax_uncertainty_mas(17.0).unwrap(), 0.070);
        assert_close(reference_parallax_uncertainty_mas(20.0).unwrap(), 0.500);
        assert_close(reference_parallax_uncertainty_mas(21.0).unwrap(), 1.300);
    }

    #[test]
    fn uses_brightness_floor_for_over_optimistic_errors() {
        let quality = parallax_quality(1.0, 0.001, 20.0).unwrap();
        assert_close(quality, 2.0);
    }

    #[test]
    fn falls_back_to_actual_error_when_brightness_is_missing() {
        let quality = parallax_quality(1.0, 0.2, f32::NAN).unwrap();
        assert_close(quality, 5.0);
    }
}
