#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Vec3 {
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

impl Vec3 {
    pub fn dot(self, other: Vec3) -> f32 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    pub fn cross(self, other: Vec3) -> Vec3 {
        Vec3 {
            x: self.y * other.z - self.z * other.y,
            y: self.z * other.x - self.x * other.z,
            z: self.x * other.y - self.y * other.x,
        }
    }

    pub fn length(self) -> f32 {
        self.dot(self).sqrt()
    }

    pub fn normalize(self) -> Option<Vec3> {
        let len = self.length();
        if !len.is_finite() || len == 0.0 {
            return None;
        }
        Some(self * (1.0 / len))
    }
}

impl std::ops::Add for Vec3 {
    type Output = Vec3;
    fn add(self, other: Vec3) -> Vec3 {
        Vec3 { x: self.x + other.x, y: self.y + other.y, z: self.z + other.z }
    }
}

impl std::ops::Sub for Vec3 {
    type Output = Vec3;
    fn sub(self, other: Vec3) -> Vec3 {
        Vec3 { x: self.x - other.x, y: self.y - other.y, z: self.z - other.z }
    }
}

impl std::ops::Mul<f32> for Vec3 {
    type Output = Vec3;
    fn mul(self, scalar: f32) -> Vec3 {
        Vec3 { x: self.x * scalar, y: self.y * scalar, z: self.z * scalar }
    }
}

impl From<[f32; 3]> for Vec3 {
    fn from(v: [f32; 3]) -> Vec3 {
        Vec3 { x: v[0], y: v[1], z: v[2] }
    }
}

impl From<Vec3> for [f32; 3] {
    fn from(v: Vec3) -> [f32; 3] {
        [v.x, v.y, v.z]
    }
}
