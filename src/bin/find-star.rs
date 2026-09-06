use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use star_dump::starcloud::{LABELS_FILENAME, StarcloudIndex, StarcloudPoint, decode_starcloud};
use star_dump::vec3::Vec3;

// bp_rp: Gaia BP-RP color index from catalog or spectral-type calibration.
// lum_min/lum_max: expected G-band luminosity range (solar units) via the
//   index formula lum = 10^((-M_G + 4.67) / 2.5). Used as a sanity gate so
//   that a close-by background star (or a companion) does not false-positive.
struct Star {
    name: &'static str,
    ra_deg: f64,
    dec_deg: f64,
    parallax_mas: f64,
    bp_rp: f32,
    lum_min: f32,
    lum_max: f32,
}

const STARS: &[Star] = &[
    // Nearby stars — well measured in Gaia DR3
    Star { name: "Proxima Centauri", ra_deg: 217.4289, dec_deg: -62.6796, parallax_mas: 768.5, bp_rp: 3.57, lum_min: 0.0001,   lum_max: 0.001     },
    Star { name: "Alpha Centauri A", ra_deg: 219.9021, dec_deg: -60.8340, parallax_mas: 747.1, bp_rp: 0.71, lum_min: 0.8,      lum_max: 2.5       },
    Star { name: "Barnard's Star",   ra_deg: 269.4521, dec_deg:   4.6933, parallax_mas: 546.9, bp_rp: 2.58, lum_min: 0.0003,   lum_max: 0.003     },
    Star { name: "Sirius",           ra_deg: 101.2872, dec_deg: -16.7161, parallax_mas: 379.2, bp_rp: -0.01, lum_min: 10.0,   lum_max: 50.0      },
    Star { name: "Epsilon Eridani",  ra_deg:  53.2327, dec_deg:  -9.4582, parallax_mas: 310.7, bp_rp: 1.10, lum_min: 0.15,    lum_max: 0.6       },
    Star { name: "Tau Ceti",         ra_deg:  26.0170, dec_deg: -15.9387, parallax_mas: 273.9, bp_rp: 1.02, lum_min: 0.3,     lum_max: 0.8       },
    // Nearby M dwarfs — faint enough for Gaia, interesting for various reasons
    Star { name: "Wolf 359",          ra_deg: 164.1208, dec_deg:   7.0145, parallax_mas: 414.3, bp_rp: 3.63, lum_min: 0.00005, lum_max: 0.0002  },
    Star { name: "Ross 128",          ra_deg: 176.9350, dec_deg:   0.8046, parallax_mas: 299.6, bp_rp: 3.11, lum_min: 0.0002,  lum_max: 0.001   },
    Star { name: "61 Cygni A",        ra_deg: 316.7248, dec_deg:  38.7494, parallax_mas: 285.9, bp_rp: 1.57, lum_min: 0.08,   lum_max: 0.25    },
    Star { name: "Kapteyn's Star",    ra_deg:  77.9190, dec_deg: -45.0178, parallax_mas: 255.1, bp_rp: 2.08, lum_min: 0.0006, lum_max: 0.003   },
    Star { name: "Teegarden's Star",  ra_deg:  43.2538, dec_deg:  16.8814, parallax_mas: 259.3, bp_rp: 3.80, lum_min: 0.0004, lum_max: 0.002   },
    Star { name: "TRAPPIST-1",        ra_deg: 346.6221, dec_deg:  -5.0414, parallax_mas:  80.45, bp_rp: 4.00, lum_min: 0.0003, lum_max: 0.001  },
    // Exoplanet hosts — historical firsts or most Earth-like
    Star { name: "51 Pegasi",         ra_deg: 344.3667, dec_deg:  20.7689, parallax_mas:  64.07, bp_rp: 0.70, lum_min: 0.8,   lum_max: 2.5    },
    Star { name: "Kepler-452",        ra_deg: 296.1063, dec_deg:  44.3211, parallax_mas:   2.32, bp_rp: 0.78, lum_min: 0.7,   lum_max: 2.5    },
    // Stars that are too bright for Gaia DR3 (G < ~3) — expected absent
    Star { name: "Vega",             ra_deg: 279.2347, dec_deg:  38.7837, parallax_mas: 130.2, bp_rp: -0.02, lum_min: 25.0,   lum_max: 70.0      },
    Star { name: "Arcturus",         ra_deg: 213.9153, dec_deg:  19.1822, parallax_mas:  88.8, bp_rp: 1.53, lum_min: 100.0,   lum_max: 300.0     },
    Star { name: "Canopus",          ra_deg:  95.9880, dec_deg: -52.6957, parallax_mas:  10.4, bp_rp: 0.41, lum_min: 5000.0,  lum_max: 20000.0   },
    Star { name: "Aldebaran",        ra_deg:  68.9802, dec_deg:  16.5093, parallax_mas:  19.3, bp_rp: 1.66, lum_min: 200.0,   lum_max: 800.0     },
    Star { name: "Antares",          ra_deg: 247.3519, dec_deg: -26.4320, parallax_mas:   5.9, bp_rp: 2.37, lum_min: 20000.0, lum_max: 150000.0  },
    Star { name: "Betelgeuse",       ra_deg:  88.7929, dec_deg:   7.4071, parallax_mas:   5.0, bp_rp: 2.17, lum_min: 30000.0, lum_max: 300000.0  },
    Star { name: "Rigel",            ra_deg:  78.6345, dec_deg:  -8.2016, parallax_mas:   3.8, bp_rp: -0.03, lum_min: 50000.0,lum_max: 300000.0  },
    Star { name: "Deneb",            ra_deg: 310.3579, dec_deg:  45.2803, parallax_mas:   1.3, bp_rp: 0.09, lum_min: 100000.0,lum_max: 400000.0  },
    Star { name: "Eta Carinae",      ra_deg: 161.2650, dec_deg: -59.6845, parallax_mas:   0.43, bp_rp: 3.00, lum_min: 1e6,   lum_max: 1e9       },
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    starcloud: PathBuf,
}

fn to_cartesian(ra_deg: f64, dec_deg: f64, parallax_mas: f64) -> Vec3 {
    let d = 1000.0 / parallax_mas;
    let ra = ra_deg.to_radians();
    let dec = dec_deg.to_radians();
    Vec3 {
        x: (d * dec.cos() * ra.cos()) as f32,
        y: (d * dec.cos() * ra.sin()) as f32,
        z: (d * dec.sin()) as f32,
    }
}

fn find_leaf(index: &StarcloudIndex, target: Vec3) -> u32 {
    let mut node_idx = 0u32;
    let mut bounds = index.bounds();
    loop {
        let node = index.nodes[node_idx as usize];
        if node.child_mask == 0 {
            return node_idx;
        }
        let mid = (bounds.min + bounds.max) * 0.5;
        let cx: u8 = if target.x >= mid.x { 1 } else { 0 };
        let cy: u8 = if target.y >= mid.y { 2 } else { 0 };
        let cz: u8 = if target.z >= mid.z { 4 } else { 0 };
        let octant = cx | cy | cz;
        if node.child_mask & (1 << octant) == 0 {
            return node_idx;
        }
        let offset = (node.child_mask & ((1u8 << octant) - 1)).count_ones();
        node_idx = node.first_child + offset;
        bounds = bounds.child_bounds(octant);
    }
}

fn nearest_in_node<'a>(
    index: &'a StarcloudIndex,
    node_idx: u32,
    target: Vec3,
) -> Option<(f32, &'a StarcloudPoint)> {
    let node = index.nodes[node_idx as usize];
    let start = node.point_first as usize;
    let end = start + node.point_count as usize;
    index.points[start..end]
        .iter()
        .map(|p| {
            let d = p.position - target;
            let dist = (d.x * d.x + d.y * d.y + d.z * d.z).sqrt();
            (dist, p)
        })
        .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
}

fn is_match(delta_pos: f32, delta_bprp: f32, lum_ok: bool) -> bool {
    delta_pos < 1.0 && delta_bprp.is_finite() && delta_bprp < 0.3 && lum_ok
}

// Stars absent from Gaia: Sun is the coordinate origin; others are too bright
// for Gaia DR3. Positions are computed from catalog RA/Dec/parallax values.
fn fixed_labels() -> Vec<(&'static str, Vec3)> {
    vec![
        ("Sun", Vec3 { x: 0.0, y: 0.0, z: 0.0 }),
    ]
}

fn write_labels(entries: &[(&str, Vec3)], path: &PathBuf) -> Result<()> {
    let mut json = String::from("{\n");
    for (i, (name, pos)) in entries.iter().enumerate() {
        let comma = if i + 1 < entries.len() { "," } else { "" };
        json.push_str(&format!(
            "  \"{name}\": [{:.4}, {:.4}, {:.4}]{comma}\n",
            pos.x, pos.y, pos.z,
        ));
    }
    json.push_str("}\n");
    fs::write(path, json)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let bytes = fs::read(&args.starcloud)?;
    let index = decode_starcloud(&bytes)?;

    println!(
        "{:<20} {:>7}  {:>8} {:>7} {:>7}  {:>8}  {:>7} {:>7} {:>7}  {:>9}",
        "star", "dist_pc", "node_idx", "Δpos_pc", "Δbp_rp",
        "exp_bprp", "fnd_bprp", "fnd_lum", "status", ""
    );
    println!("{}", "-".repeat(100));

    let mut confirmed: Vec<(&str, Vec3)> = Vec::new();

    for star in STARS {
        let target = to_cartesian(star.ra_deg, star.dec_deg, star.parallax_mas);
        let dist_pc = 1000.0 / star.parallax_mas;

        let out_of_bounds = target.x.abs() > index.half_extent_pc
            || target.y.abs() > index.half_extent_pc
            || target.z.abs() > index.half_extent_pc;

        if out_of_bounds {
            println!(
                "{:<20} {:>7.1}  {:>8}  {:>7}  {:>7}  {:>8}  {:>7}  {:>7}  out-of-bounds",
                star.name, dist_pc, "-", "-", "-", "-", "-", "-"
            );
            continue;
        }

        let node_idx = find_leaf(&index, target);
        match nearest_in_node(&index, node_idx, target) {
            None => println!(
                "{:<20} {:>7.1}  {:>8}  {:>7}  {:>7}  {:>8.2}  {:>7}  {:>7}  empty-leaf",
                star.name, dist_pc, node_idx, "-", "-", star.bp_rp, "-", "-"
            ),
            Some((delta_pos, point)) => {
                let delta_bprp = if point.bp_rp.is_nan() {
                    f32::NAN
                } else {
                    (point.bp_rp - star.bp_rp).abs()
                };
                let lum_ok = point.luminosity >= star.lum_min && point.luminosity <= star.lum_max;
                let matched = is_match(delta_pos, delta_bprp, lum_ok);
                let status = if matched {
                    confirmed.push((star.name, target));
                    "MATCH"
                } else if delta_pos < 1.0 {
                    "pos-ok/mismatch"
                } else {
                    "absent"
                };
                let bprp_str = if point.bp_rp.is_nan() {
                    "  NaN".to_string()
                } else {
                    format!("{:>7.2}", point.bp_rp)
                };
                let dbprp_str = if delta_bprp.is_nan() {
                    "  NaN".to_string()
                } else {
                    format!("{:>7.2}", delta_bprp)
                };
                println!(
                    "{:<20} {:>7.1}  {:>8}  {:>7.3}  {}  {:>8.2}  {}  {:>7.4}  {}",
                    star.name, dist_pc, node_idx,
                    delta_pos, dbprp_str,
                    star.bp_rp, bprp_str, point.luminosity,
                    status
                );
            }
        }
    }

    let labels_path = args.starcloud.with_file_name(LABELS_FILENAME);
    let confirmed_count = confirmed.len();
    let mut all_labels: Vec<(&str, Vec3)> = confirmed;
    all_labels.extend(fixed_labels());
    write_labels(&all_labels, &labels_path)?;
    println!("\nWrote {} confirmed label(s) to {}", confirmed_count, labels_path.display());

    Ok(())
}
