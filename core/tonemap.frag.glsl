precision highp float;

uniform sampler2D hdr;

varying vec2 vUv;

void main() {
  vec3 color = texture2D(hdr, vUv).rgb;
  gl_FragColor = vec4(pow(color / (1.0 + color), vec3(1.0 / 2.2)), 1.0);
}
