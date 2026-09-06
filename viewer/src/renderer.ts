// WebGL2 star renderer: additive Gaussian splats accumulated into a half-float
// HDR target, then Reinhard tone mapped and gamma corrected to the screen.

import { Mat4 } from "./mat4";
import { POINT_BYTES } from "./starcloud";
import { Vec3 } from "./vec3";
import starsVert from "./stars.vert.glsl";
import starsFrag from "./stars.frag.glsl";
import tonemapVert from "./tonemap.vert.glsl";
import tonemapFrag from "./tonemap.frag.glsl";

const EXPOSURE      = 2000;
const SIZE_SCALE    = 2;
const MAX_RADIUS_PX = 1.5;

export type Renderer = {
  resize(width: number, height: number): void;
  upload(batch: number, data: ArrayBuffer): void;
  free(batch: number): void;
  /** `ranges` holds [batch, firstPoint, pointCount] triples. */
  render(projection: Mat4, view: Mat4, eye: Vec3, ranges: Int32Array): void;
};

function compile(gl: WebGL2RenderingContext, type: number, source: string): WebGLShader {
  const shader = gl.createShader(type)!;
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    throw new Error(gl.getShaderInfoLog(shader) ?? "shader compile failed");
  }
  return shader;
}

function link(gl: WebGL2RenderingContext, vert: string, frag: string): WebGLProgram {
  const program = gl.createProgram()!;
  gl.attachShader(program, compile(gl, gl.VERTEX_SHADER, vert));
  gl.attachShader(program, compile(gl, gl.FRAGMENT_SHADER, frag));
  gl.linkProgram(program);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    throw new Error(gl.getProgramInfoLog(program) ?? "program link failed");
  }
  return program;
}

export function createRenderer(canvas: HTMLCanvasElement): Renderer {
  const gl = canvas.getContext("webgl2", { antialias: false, alpha: false, depth: false });
  if (!gl) throw new Error("WebGL2 is required");
  if (!gl.getExtension("EXT_color_buffer_half_float") && !gl.getExtension("EXT_color_buffer_float")) {
    throw new Error("half float render targets are required");
  }

  const stars = link(gl, starsVert, starsFrag);
  const tonemap = link(gl, tonemapVert, tonemapFrag);
  const uniform = (program: WebGLProgram, name: string) => gl.getUniformLocation(program, name);
  const uProjection = uniform(stars, "projection");
  const uView       = uniform(stars, "view");
  const uEye        = uniform(stars, "eye");
  const uExposure   = uniform(stars, "exposure");
  const uSizeScale  = uniform(stars, "sizeScale");
  const uMaxRadius  = uniform(stars, "maxRadius");

  const aPosition   = gl.getAttribLocation(stars, "position");
  const aLuminosity = gl.getAttribLocation(stars, "luminosity");
  const aBpRp       = gl.getAttribLocation(stars, "bpRp");

  const vao = gl.createVertexArray();
  gl.bindVertexArray(vao);
  for (const attribute of [aPosition, aLuminosity, aBpRp]) gl.enableVertexAttribArray(attribute);

  const hdr = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, hdr);
  for (const parameter of [gl.TEXTURE_MIN_FILTER, gl.TEXTURE_MAG_FILTER]) {
    gl.texParameteri(gl.TEXTURE_2D, parameter, gl.NEAREST);
  }
  for (const parameter of [gl.TEXTURE_WRAP_S, gl.TEXTURE_WRAP_T]) {
    gl.texParameteri(gl.TEXTURE_2D, parameter, gl.CLAMP_TO_EDGE);
  }
  const framebuffer = gl.createFramebuffer();
  gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);
  gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, hdr, 0);
  gl.bindFramebuffer(gl.FRAMEBUFFER, null);

  const buffers = new Map<number, WebGLBuffer>();
  let width = 1;
  let height = 1;

  const bind = (batch: number) => {
    gl.bindBuffer(gl.ARRAY_BUFFER, buffers.get(batch)!);
    gl.vertexAttribPointer(aPosition,   3, gl.FLOAT, false, POINT_BYTES, 0);
    gl.vertexAttribPointer(aLuminosity, 1, gl.FLOAT, false, POINT_BYTES, 12);
    gl.vertexAttribPointer(aBpRp,       1, gl.FLOAT, false, POINT_BYTES, 16);
  };

  return {
    resize(w, h) {
      width = w;
      height = h;
      gl.bindTexture(gl.TEXTURE_2D, hdr);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA16F, w, h, 0, gl.RGBA, gl.HALF_FLOAT, null);
    },

    upload(batch, data) {
      const buffer = gl.createBuffer()!;
      gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
      gl.bufferData(gl.ARRAY_BUFFER, data, gl.STATIC_DRAW);
      buffers.set(batch, buffer);
    },

    free(batch) {
      gl.deleteBuffer(buffers.get(batch)!);
      buffers.delete(batch);
    },

    render(projection, view, eye, ranges) {
      gl.viewport(0, 0, width, height);
      gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);
      gl.clearColor(0, 0, 0, 1);
      gl.clear(gl.COLOR_BUFFER_BIT);

      gl.useProgram(stars);
      gl.uniformMatrix4fv(uProjection, false, projection);
      gl.uniformMatrix4fv(uView, false, view);
      gl.uniform3fv(uEye, eye);
      gl.uniform1f(uExposure, EXPOSURE);
      gl.uniform1f(uSizeScale, SIZE_SCALE);
      gl.uniform1f(uMaxRadius, MAX_RADIUS_PX);
      gl.enable(gl.BLEND);
      gl.blendFunc(gl.ONE, gl.ONE);
      gl.bindVertexArray(vao);

      let bound = -1;
      for (let i = 0; i < ranges.length; i += 3) {
        if (ranges[i] !== bound) {
          bound = ranges[i];
          bind(bound);
        }
        gl.drawArrays(gl.POINTS, ranges[i + 1], ranges[i + 2]);
      }
      gl.disable(gl.BLEND);

      gl.bindFramebuffer(gl.FRAMEBUFFER, null);
      gl.bindVertexArray(null);
      gl.useProgram(tonemap);
      gl.bindTexture(gl.TEXTURE_2D, hdr);
      gl.drawArrays(gl.TRIANGLES, 0, 3);
    },
  };
}
