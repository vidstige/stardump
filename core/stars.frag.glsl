#version 300 es
precision highp float;

in vec3 vColor;
in float vBrightness;
in float vGaussCoeff;

out vec4 fragColor;

void main() {
  vec2 d = gl_PointCoord - 0.5;
  float val = vBrightness * exp(-dot(d, d) * vGaussCoeff);
  // Negated so that a NaN, which compares false against everything, is dropped.
  if (!(val > 1e-6)) discard;
  fragColor = vec4(vColor * val, 1.0);
}
