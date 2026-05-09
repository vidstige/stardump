precision highp float;

attribute vec3 position;
attribute float luminosity;
attribute float bpRp;

uniform mat4 projection;
uniform mat4 view;
uniform vec3 cameraPosition;
uniform float exposure;
uniform float uSizeScale;
uniform float uMaxRadius;

varying vec3 vColor;
varying float vBrightness;
varying float vGaussCoeff;

vec3 bpRpToColor(float t) {
  vec3 blue   = vec3(0.6, 0.7, 1.0);
  vec3 white  = vec3(1.0, 0.95, 0.9);
  vec3 yellow = vec3(1.0, 0.85, 0.4);
  vec3 red    = vec3(1.0, 0.3,  0.1);
  if (t < 0.33) return mix(blue,   white,  t / 0.33);
  if (t < 0.66) return mix(white,  yellow, (t - 0.33) / 0.33);
                return mix(yellow, red,    (t - 0.66) / 0.34);
}

void main() {
  gl_Position = projection * view * vec4(position, 1.0);
  float dist = length(position - cameraPosition);
  float flux = luminosity / max(dist * dist, 0.01);
  float brightness = flux * exposure;
  float t = clamp((bpRp + 0.5) / 3.5, 0.0, 1.0);
  vColor = (bpRp != bpRp) ? vec3(1.0) : bpRpToColor(t);

  // Match render-fast.ts: radius scales with brightness, clamped to [0.8, uMaxRadius]
  float rPx = clamp(brightness * uSizeScale, 0.8, uMaxRadius);
  float spriteSizePx = ceil(rPx) * 2.0 + 1.0;
  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  // Convert Gaussian coeff from pixel² to gl_PointCoord² space
  vGaussCoeff = 4.0 * spriteSizePx * spriteSizePx / (rPx * rPx);
}
