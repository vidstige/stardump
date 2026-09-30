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

varying vec3 vColor;
varying float vBrightness;
varying float vGaussCoeff;

/** Colour of a star with no measured bp_rp: the white point of the ramp. */
const float UNKNOWN_COLOR = 1.0 / 3.0;

/**
 * Half the sprite, in standard deviations of the gaussian drawn on it. What is
 * seen of a star is not its sigma but the disc out to where its tail crosses
 * the white point, and that radius is sigma times the square root of twice the
 * log of its brightness — near five sigma for the brightest thing in the sky
 * here. Any less and the disc is clipped into the square it is drawn on.
 */
const float SPREAD = 5.0;

/** Smallest star, which is nearly all of them, matching the old faint dot. */
const float MIN_SIGMA = 0.32;

const float LOG10 = 0.4342944819;

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

  // Sized by the log of brightness, the way a chart sizes a star by magnitude.
  // Screen brightness runs over five decades in one frame — a star half a
  // parsec off against the field behind it — so anything proportional either
  // leaves the near ones the same as the far ones or turns the whole sky into
  // discs. A decade of brightness is worth sizeScale standard deviations.
  float sigma = clamp(sizeScale * log(1.0 + brightness) * LOG10, MIN_SIGMA, maxRadius);
  float spriteSizePx = 2.0 * SPREAD * sigma + 1.0;
  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  vGaussCoeff = spriteSizePx * spriteSizePx / (2.0 * sigma * sigma);
}
