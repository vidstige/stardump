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

fn expand_bits(value: u32) -> u32 {
    let mut value = value & 0x3f;
    value = (value | (value << 8)) & 0x0000_f00f;
    value = (value | (value << 4)) & 0x000c_30c3;
    value = (value | (value << 2)) & 0x0024_9249;
    value
}

fn compact_bits(value: u32) -> u32 {
    let mut value = value & 0x0024_9249;
    value = (value ^ (value >> 2)) & 0x000c_30c3;
    value = (value ^ (value >> 4)) & 0x0000_f00f;
    value = (value ^ (value >> 8)) & 0x0000_003f;
    value
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
    expand_bits(x) | (expand_bits(y) << 1) | (expand_bits(z) << 2)
}

pub fn morton_decode(code: u32) -> [u32; 3] {
    [
        compact_bits(code),
        compact_bits(code >> 1),
        compact_bits(code >> 2),
    ]
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
}

impl OctreeConfig {
    pub fn morton_for_point(&self, point: [f32; 3]) -> Option<u32> {
        let scale = 1_u32 << self.depth;
        Some(morton_encode(
            axis_index(point[0], self.bounds.min[0], self.bounds.max[0], scale)?,
            axis_index(point[1], self.bounds.min[1], self.bounds.max[1], scale)?,
            axis_index(point[2], self.bounds.min[2], self.bounds.max[2], scale)?,
        ))
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
}
