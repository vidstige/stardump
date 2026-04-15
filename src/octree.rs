#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bounds3 {
    pub min: [f32; 3],
    pub max: [f32; 3],
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OctreeConfig {
    pub depth: u8,
    pub bounds: Bounds3,
}

fn axis_index(value: f32, min: f32, max: f32, scale: u32) -> Option<u32> {
    if !value.is_finite() || value < min || value > max {
        return None;
    }

    if value == max {
        return Some(scale - 1);
    }

    let fraction = (value - min) / (max - min);
    Some((fraction * scale as f32).floor() as u32)
}

fn axis_range(
    min_value: f32,
    max_value: f32,
    min: f32,
    max: f32,
    scale: u32,
) -> Option<(u32, u32)> {
    if !min_value.is_finite() || !max_value.is_finite() || min_value > max_value {
        return None;
    }
    if max_value < min || min_value > max {
        return None;
    }

    let clamped_min = min_value.max(min);
    let clamped_max = max_value.min(max);
    let cell = (max - min) / scale as f32;

    let start = if clamped_min <= min {
        0
    } else {
        ((((clamped_min - min) / cell).ceil() as i64) - 1).clamp(0, scale as i64 - 1) as u32
    };
    let end = if clamped_max >= max {
        scale - 1
    } else {
        (((clamped_max - min) / cell).floor() as i64).clamp(0, scale as i64 - 1) as u32
    };

    Some((start, end))
}

fn clamp_distance(value: f32, min: f32, max: f32) -> f32 {
    if value < min {
        min - value
    } else if value > max {
        value - max
    } else {
        0.0
    }
}

pub fn morton_encode(x: u32, y: u32, z: u32) -> u32 {
    let mut code = 0_u32;
    for bit in 0..10 {
        code |= ((x >> bit) & 1) << (3 * bit);
        code |= ((y >> bit) & 1) << (3 * bit + 1);
        code |= ((z >> bit) & 1) << (3 * bit + 2);
    }
    code
}

pub fn morton_decode(code: u32) -> [u32; 3] {
    let mut x = 0_u32;
    let mut y = 0_u32;
    let mut z = 0_u32;
    for bit in 0..10 {
        x |= ((code >> (3 * bit)) & 1) << bit;
        y |= ((code >> (3 * bit + 1)) & 1) << bit;
        z |= ((code >> (3 * bit + 2)) & 1) << bit;
    }
    [x, y, z]
}

impl Bounds3 {
    pub fn cube_size(&self) -> f32 {
        self.max[0] - self.min[0]
    }

    pub fn cell_size(&self, depth: u8) -> f32 {
        self.cube_size() / (1_u32 << depth) as f32
    }

    pub fn leaf_bounds(&self, depth: u8, morton: u32) -> Bounds3 {
        let [x, y, z] = morton_decode(morton);
        let cell = self.cell_size(depth);
        Bounds3 {
            min: [
                self.min[0] + x as f32 * cell,
                self.min[1] + y as f32 * cell,
                self.min[2] + z as f32 * cell,
            ],
            max: [
                self.min[0] + (x + 1) as f32 * cell,
                self.min[1] + (y + 1) as f32 * cell,
                self.min[2] + (z + 1) as f32 * cell,
            ],
        }
    }

    pub fn intersects_sphere(&self, center: [f32; 3], radius: f32) -> bool {
        let dx = clamp_distance(center[0], self.min[0], self.max[0]);
        let dy = clamp_distance(center[1], self.min[1], self.max[1]);
        let dz = clamp_distance(center[2], self.min[2], self.max[2]);
        dx * dx + dy * dy + dz * dz <= radius * radius
    }

    pub fn child_bounds(&self, child: u8) -> Bounds3 {
        let mid = [
            (self.min[0] + self.max[0]) * 0.5,
            (self.min[1] + self.max[1]) * 0.5,
            (self.min[2] + self.max[2]) * 0.5,
        ];
        Bounds3 {
            min: [
                if child & 1 == 0 { self.min[0] } else { mid[0] },
                if child & 2 == 0 { self.min[1] } else { mid[1] },
                if child & 4 == 0 { self.min[2] } else { mid[2] },
            ],
            max: [
                if child & 1 == 0 { mid[0] } else { self.max[0] },
                if child & 2 == 0 { mid[1] } else { self.max[1] },
                if child & 4 == 0 { mid[2] } else { self.max[2] },
            ],
        }
    }
}

impl OctreeConfig {
    pub fn axis_scale(&self) -> u32 {
        1_u32 << self.depth
    }

    pub fn morton_for_point(&self, point: [f32; 3]) -> Option<u32> {
        let scale = self.axis_scale();
        Some(morton_encode(
            axis_index(point[0], self.bounds.min[0], self.bounds.max[0], scale)?,
            axis_index(point[1], self.bounds.min[1], self.bounds.max[1], scale)?,
            axis_index(point[2], self.bounds.min[2], self.bounds.max[2], scale)?,
        ))
    }

    pub fn leaf_ranges_for_bounds(&self, min: [f32; 3], max: [f32; 3]) -> Option<[(u32, u32); 3]> {
        let scale = self.axis_scale();
        Some([
            axis_range(
                min[0],
                max[0],
                self.bounds.min[0],
                self.bounds.max[0],
                scale,
            )?,
            axis_range(
                min[1],
                max[1],
                self.bounds.min[1],
                self.bounds.max[1],
                scale,
            )?,
            axis_range(
                min[2],
                max[2],
                self.bounds.min[2],
                self.bounds.max[2],
                scale,
            )?,
        ])
    }

    pub fn leaf_bounds(&self, morton: u32) -> Bounds3 {
        self.bounds.leaf_bounds(self.depth, morton)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn morton_round_trip_preserves_axis_indices() {
        let code = morton_encode(3, 17, 42);
        assert_eq!(morton_decode(code), [3, 17, 42]);
    }

    #[test]
    fn morton_round_trip_preserves_seven_bit_values() {
        let code = morton_encode(127, 126, 125);
        assert_eq!(morton_decode(code), [127, 126, 125]);
    }

    #[test]
    fn maps_points_into_fixed_depth_leaves() {
        let config = OctreeConfig {
            depth: 6,
            bounds: Bounds3 {
                min: [-100_000.0, -100_000.0, -100_000.0],
                max: [100_000.0, 100_000.0, 100_000.0],
            },
        };

        let morton = config.morton_for_point([0.0, 0.0, 0.0]).unwrap();
        let bounds = config.leaf_bounds(morton);

        assert!(bounds.min[0] <= 0.0);
        assert!(bounds.max[0] >= 0.0);
        assert!(bounds.intersects_sphere([0.0, 0.0, 0.0], 1.0));
        assert!(!bounds.intersects_sphere([200_000.0, 0.0, 0.0], 1.0));
    }

    #[test]
    fn leaf_ranges_include_cells_touching_query_bounds() {
        let config = OctreeConfig {
            depth: 1,
            bounds: Bounds3 {
                min: [0.0, 0.0, 0.0],
                max: [2.0, 2.0, 2.0],
            },
        };

        let ranges = config
            .leaf_ranges_for_bounds([1.0, 1.0, 1.0], [1.0, 1.0, 1.0])
            .unwrap();

        assert_eq!(ranges, [(0, 1), (0, 1), (0, 1)]);
    }
}
