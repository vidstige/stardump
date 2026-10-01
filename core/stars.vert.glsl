precision highp float;

attribute vec3 position;
attribute float luminosity;
attribute float bpRp;

uniform mat4 projection;
uniform mat4 view;
uniform vec3 eye;
uniform float exposure;
uniform float sizeScale;
uniform float maxRadius;
/** The largest sprite the driver will give us: 64 px headless, 1023 in Chrome. */
uniform float maxPointSize;

varying vec3 vColor;
varying float vBrightness;
varying float vGaussCoeff;

/** Colour of a star with no measured bp_rp: the white point of the ramp. */
const float UNKNOWN_COLOR = 1.0 / 3.0;

/** Standard deviations per unit of radius, which fixes the shape of a star. */
const float SIGMA_PER_RADIUS = 0.35355339;

/** How far down the tail has to go before the quad may end: one output level. */
const float LEVELS = 255.0;

vec3 bpRpToColor(float t) {
  float s = t * 3.0;
  vec3 blue   = vec3(0.6, 0.7, 1.0);
  vec3 white  = vec3(1.0, 0.95, 0.9);
  vec3 yellow = vec3(1.0, 0.85, 0.4);
  vec3 red    = vec3(1.0, 0.3,  0.1);
  vec3 col = mix(blue,   white,  clamp(s,       0.0, 1.0));
       col = mix(col,    yellow, clamp(s - 1.0, 0.0, 1.0));
       col = mix(col,    red,    clamp(s - 2.0, 0.0, 1.0));
  return col;
}

void main() {
  gl_Position = projection * view * vec4(position, 1.0);
  vec3 delta = position - eye;
  float brightness = luminosity * exposure / max(dot(delta, delta), 0.01);

  // A small fraction of Gaia sources have no bp_rp at all. NaN would survive
  // the clamp and spread through the additive buffer, blanking out every pixel
  // the splat lands on, so those stars are drawn white instead.
  float t = bpRp == bpRp ? clamp((bpRp + 0.5) / 3.5, 0.0, 1.0) : UNKNOWN_COLOR;
  vColor = bpRpToColor(t);

  float rPx = clamp(brightness * sizeScale, 0.8, maxRadius);

  // What is seen of a star is not its sigma but the disc out to where its tail
  // crosses the white point, and the quad has to hold that or the disc is
  // clipped into the square it is drawn on. Sizing the quad by a fixed number
  // of sigma, as this used to, clips every star over a brightness of 55
  // whatever its radius. What has to clear the edge is the whole saturated
  // disc and not merely the corner: half a quad of sigma * sqrt(2 * log(LEVELS
  // * brightness)) puts every side of it past where the tail has fallen below
  // one output level. For the brightest star here that is a quad near four
  // times its radius, and for the faint ones, which are almost all of them,
  // the max below leaves it exactly as it was.
  float sigma = rPx * SIGMA_PER_RADIUS;
  float reach = sigma * sqrt(2.0 * log(LEVELS * brightness + 1.0));
  float spriteSizePx = min(2.0 * max(reach, rPx) + 1.0, maxPointSize);

  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  // From the size actually granted, not the one asked for: a driver that caps
  // the sprite would otherwise leave the gaussian too narrow for its own quad.
  vGaussCoeff = spriteSizePx * spriteSizePx / (2.0 * sigma * sigma);
}
