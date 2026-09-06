#version 300 es
precision highp float;

uniform sampler2D hdr;
in vec2 vUv;
out vec4 fragColor;

void main() {
  vec3 color = texture(hdr, vUv).rgb;
  fragColor = vec4(pow(color / (1.0 + color), vec3(1.0 / 2.2)), 1.0);
}
