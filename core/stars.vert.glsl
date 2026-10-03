precision highp float;

attribute vec3 position;
attribute float luminosity;
attribute float bpRp;

uniform mat4 projection;
uniform mat4 view;
uniform vec3 eye;
uniform float exposure;
uniform float sizeScale;
uniform float minRadius;
uniform float maxRadius;
/** The largest sprite the driver will give us: 64 px headless, 1023 in Chrome. */
uniform float maxPointSize;
/**
 * A span standing in for a subtree that is still streaming. standMask holds
 * one bit per octant of the box around standCenter that finer data already
 * covers, and the span draws only in the others; zero for an ordinary span,
 * which is every span once a view has fully loaded. standBoost is the
 * multiplier the index baked into the luminosity of this subsample, one for
 * an ordinary span, and standPower how much of it is kept: at 1 the
 * subsample is shown as stored, carrying the flux of every star in its box,
 * at 0 its stars show at their own brightness. standHalf is half the side of
 * the box, so that the boost can be faded out for a box the camera is near.
 */
uniform vec3 standCenter;
uniform float standHalf;
uniform float standBoost;
uniform float standPower;
uniform float standMask;

/**
 * The boost conserves luminosity, which is flux only when the whole box is
 * about equally far away. From STAND_FAR half-sizes out it is kept whole;
 * inside STAND_NEAR, where a nearby sample would otherwise carry the light of
 * stars thousands of parsecs behind it, it is dropped and the sample shows as
 * the one star it is.
 */
const float STAND_NEAR = 1.5;
const float STAND_FAR = 3.0;

/**
 * The largest splat a stand-in star may make, in pixels of radius. The boost
 * is right on average but a boosted giant is a thousand giants of light in
 * one point, a saturated disc that pops out and then shrinks when its leaf
 * arrives. The faint majority are far below this and keep their full boost;
 * only the few that would turn into discs are held back, and the real giant
 * among them comes in at its own size with the rest of its box.
 */
const float STAND_MAX_RADIUS = 2.0;

varying vec3 vColor;
varying float vBrightness;
varying float vGaussCoeff;

/** Colour of a star with no measured bp_rp: the white point of the ramp. */
const float UNKNOWN_COLOR = 1.0 / 3.0;

/**
 * Closest a star is allowed to get before the inverse square stops, in square
 * parsecs. It is only there so that sitting exactly on one cannot divide by
 * zero. At the 0.01 it used to be, no approach inside 0.1 pc gained any
 * brightness at all, which is most of the range a camera holding a star works
 * in: a red dwarf held at 0.05 pc came out no brighter than at 0.1.
 */
const float NEAREST_PC2 = 1.0e-6;

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

/**
 * Whether the octant this point of a stand-in falls in is already drawn by
 * finer data. Always false for an ordinary span.
 */
bool hidden() {
  if (standMask == 0.0) return false;
  vec3 upper = step(standCenter, position);
  float octant = upper.x + 2.0 * upper.y + 4.0 * upper.z;
  return mod(floor(standMask / exp2(octant)), 2.0) > 0.5;
}

void main() {
  if (hidden()) {
    // Behind the far plane, and dark besides, so neither pass can show it.
    gl_Position = vec4(0.0, 0.0, 2.0, 1.0);
    gl_PointSize = 1.0;
    vColor = vec3(0.0);
    vBrightness = 0.0;
    vGaussCoeff = 1.0;
    return;
  }
  gl_Position = projection * view * vec4(position, 1.0);
  vec3 delta = position - eye;
  float farness = clamp((length(standCenter - eye) / standHalf - STAND_NEAR) / (STAND_FAR - STAND_NEAR), 0.0, 1.0);
  float boost = pow(standBoost, standPower * farness - 1.0);
  float brightness = luminosity * boost * exposure / max(dot(delta, delta), NEAREST_PC2);
  if (standMask > 0.0) brightness = min(brightness, STAND_MAX_RADIUS / sizeScale);

  // A small fraction of Gaia sources have no bp_rp at all. NaN would survive
  // the clamp and spread through the additive buffer, blanking out every pixel
  // the splat lands on, so those stars are drawn white instead.
  float t = bpRp == bpRp ? clamp((bpRp + 0.5) / 3.5, 0.0, 1.0) : UNKNOWN_COLOR;
  vColor = bpRpToColor(t);

  float rPx = clamp(brightness * sizeScale, minRadius, maxRadius);
  // Floored rather than allowed to reach zero: the coefficient divides by its
  // square, and an infinite one turns the centre of the splat into a NaN that
  // the fragment shader then throws away, losing the star altogether.
  float sigma = max(rPx * SIGMA_PER_RADIUS, 1.0e-3);

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
  float reach = sigma * sqrt(2.0 * log(LEVELS * brightness + 1.0));
  float spriteSizePx = clamp(2.0 * max(reach, rPx) + 1.0, 1.0, maxPointSize);

  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  // From the size actually granted, not the one asked for: a driver that caps
  // the sprite would otherwise leave the gaussian too narrow for its own quad.
  vGaussCoeff = spriteSizePx * spriteSizePx / (2.0 * sigma * sigma);
}
