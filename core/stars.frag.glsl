precision highp float;

varying vec3 vColor;
varying float vBrightness;
varying float vGaussCoeff;

void main() {
  vec2 d = gl_PointCoord - 0.5;
  float val = vBrightness * exp(-dot(d, d) * vGaussCoeff);
  // Negated so that a NaN, which compares false against everything, is dropped.
  if (!(val > 1e-6)) discard;
  gl_FragColor = vec4(vColor * val, 1.0);
}
