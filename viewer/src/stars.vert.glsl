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
  vec3 delta = position - cameraPosition;
  float brightness = luminosity * exposure / max(dot(delta, delta), 0.01);

  float t = clamp((bpRp + 0.5) / 3.5, 0.0, 1.0);
  vColor = bpRpToColor(t);

  float rPx = clamp(brightness * uSizeScale, 0.8, uMaxRadius);
  float spriteSizePx = rPx * 2.0 + 1.0;
  gl_PointSize = spriteSizePx;
  vBrightness = brightness;
  vGaussCoeff = 4.0 * spriteSizePx * spriteSizePx / (rPx * rPx);
}
