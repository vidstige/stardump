#version 300 es
precision highp float;

in vec3 position;
in float luminosity;
in float bpRp;

uniform mat4 projection;
uniform mat4 view;
uniform vec3 eye;
uniform float exposure;
uniform float sizeScale;
uniform float maxRadius;

out vec3 vColor;
out float vBrightness;
out float vGaussCoeff;

/** Colour of a star with no measured bp_rp: the white point of the ramp. */
const float UNKNOWN_COLOR = 1.0 / 3.0;

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
  float spriteSizePx = rPx * 2.0 + 1.0;
  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  vGaussCoeff = 4.0 * spriteSizePx * spriteSizePx / (rPx * rPx);
}
