precision highp float;
uniform sampler2D uHdr;
varying vec2 vUv;
void main() {
  vec3 hdr = texture2D(uHdr, vUv).rgb;
  gl_FragColor = vec4(pow(hdr / (1.0 + hdr), vec3(1.0 / 2.2)), 1.0);
}
