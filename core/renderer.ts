// Star renderer: additive Gaussian splats accumulated into a floating point
// HDR target, then Reinhard tone mapped and gamma corrected to the drawing
// buffer. WebGL 1.0 / GLSL ES 1.00, which is the ceiling headless-gl offers,
// so the browser and Node run the same shaders.

import { STAND_FLOATS } from "./lod";
import { Mat4 } from "./mat4";
import { Settings } from "./settings";
import { POINT_BYTES } from "./starcloud";
import { Vec3 } from "./vec3";

/** The four shader sources, however the host happens to obtain them. */
export type Sources = {
  starsVert: string;
  starsFrag: string;
  tonemapVert: string;
  tonemapFrag: string;
};

export type Renderer = {
  resize(width: number, height: number): void;
  upload(batch: number, data: ArrayBuffer): void;
  free(batch: number): void;
  /**
   * A pass is begin, any number of draws, end. Splats accumulate additively
   * with no depth test, so the draws are order independent and may be split
   * however the caller likes — which is what lets an offline frame upload its
   * stars a chunk at a time and free each one before the next, instead of
   * holding the whole cut on the GPU at once.
   */
  begin(projection: Mat4, view: Mat4, eye: Vec3, settings: Settings): void;
  /**
   * `ranges` holds [batch, firstPoint, pointCount] triples. `stands`, when
   * given, holds STAND_FLOATS per span, marking spans that stand in for
   * subtrees still streaming; see Draws in lod.ts. Without it every span is
   * drawn whole, which is all an offline frame ever needs.
   */
  draw(ranges: Int32Array, stands?: Float32Array): void;
  end(): void;
  /** The whole pass at once, for a caller that already has everything. */
  render(
    projection: Mat4, view: Mat4, eye: Vec3, ranges: Int32Array, settings: Settings,
    stands?: Float32Array,
  ): void;
};

// Explicit and disjoint, so the two programs never fight over a slot and each
// pass can simply enable its own attributes and disable the others.
const POSITION   = 0;
const LUMINOSITY = 1;
const BP_RP      = 2;
const QUAD       = 3;

function compile(gl: WebGLRenderingContext, type: number, source: string): WebGLShader {
  const shader = gl.createShader(type)!;
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    throw new Error(gl.getShaderInfoLog(shader) ?? "shader compile failed");
  }
  return shader;
}

function link(
  gl: WebGLRenderingContext, vert: string, frag: string, locations: Record<string, number>,
): WebGLProgram {
  const program = gl.createProgram()!;
  gl.attachShader(program, compile(gl, gl.VERTEX_SHADER, vert));
  gl.attachShader(program, compile(gl, gl.FRAGMENT_SHADER, frag));
  for (const [name, slot] of Object.entries(locations)) {
    gl.bindAttribLocation(program, slot, name);
  }
  gl.linkProgram(program);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    throw new Error(gl.getProgramInfoLog(program) ?? "program link failed");
  }
  return program;
}

const HALF_FLOAT_OES = 0x8d61;

/**
 * Pixel type for the accumulation buffer. Half float is half the memory and
 * bandwidth and is what browsers, phones especially, are happiest rendering
 * to; headless-gl offers no half float at all but does render to full float,
 * despite advertising none of the colour buffer extensions.
 */
function hdrType(gl: WebGLRenderingContext): number {
  const half = gl.getExtension("OES_texture_half_float") &&
    gl.getExtension("EXT_color_buffer_half_float");
  if (half) return HALF_FLOAT_OES;
  if (gl.getExtension("OES_texture_float")) return gl.FLOAT;
  throw new Error("floating point render targets are required");
}

export function createRenderer(gl: WebGLRenderingContext, sources: Sources): Renderer {
  const type = hdrType(gl);
  const stars = link(gl, sources.starsVert, sources.starsFrag, {
    position: POSITION, luminosity: LUMINOSITY, bpRp: BP_RP,
  });
  const tonemap = link(gl, sources.tonemapVert, sources.tonemapFrag, { position: QUAD });

  const uniform = (program: WebGLProgram, name: string) => gl.getUniformLocation(program, name);
  const uProjection = uniform(stars, "projection");
  const uView       = uniform(stars, "view");
  const uEye        = uniform(stars, "eye");
  const uExposure   = uniform(stars, "exposure");
  const uSizeScale  = uniform(stars, "sizeScale");
  const uMaxRadius  = uniform(stars, "maxRadius");
  const uMaxPoint   = uniform(stars, "maxPointSize");
  const uStandCenter = uniform(stars, "standCenter");
  const uStandBoost  = uniform(stars, "standBoost");
  const uStandMask   = uniform(stars, "standMask");

  // headless-gl grants 64 px, Chrome 1023, and the shader has to know which so
  // that it sizes the gaussian to the quad it will really get.
  const maxPointSize = gl.getParameter(gl.ALIASED_POINT_SIZE_RANGE)[1];

  // One triangle large enough to cover the viewport, so the tone map pass
  // needs no index buffer and no second draw.
  const quad = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, quad);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 3, -1, -1, 3]), gl.STATIC_DRAW);

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
    gl.vertexAttribPointer(POSITION,   3, gl.FLOAT, false, POINT_BYTES, 0);
    gl.vertexAttribPointer(LUMINOSITY, 1, gl.FLOAT, false, POINT_BYTES, 12);
    gl.vertexAttribPointer(BP_RP,      1, gl.FLOAT, false, POINT_BYTES, 16);
  };

  return {
    resize(w, h) {
      width = w;
      height = h;
      gl.bindTexture(gl.TEXTURE_2D, hdr);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, w, h, 0, gl.RGBA, type, null);
    },

    // Uploading over a batch reuses its buffer rather than making a new one.
    // Deleting a buffer and immediately creating another loses the draws that
    // used it under headless-gl, so a caller that streams the same slot over
    // and over — an offline frame drawing its cut a chunk at a time — must be
    // able to re-specify one rather than churn through names.
    upload(batch, data) {
      const buffer = buffers.get(batch) ?? gl.createBuffer()!;
      gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
      gl.bufferData(gl.ARRAY_BUFFER, data, gl.STATIC_DRAW);
      buffers.set(batch, buffer);
    },

    free(batch) {
      gl.deleteBuffer(buffers.get(batch)!);
      buffers.delete(batch);
    },

    begin(projection, view, eye, settings) {
      gl.viewport(0, 0, width, height);
      gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);
      gl.clearColor(0, 0, 0, 1);
      gl.clear(gl.COLOR_BUFFER_BIT);

      gl.useProgram(stars);
      gl.uniformMatrix4fv(uProjection, false, projection);
      gl.uniformMatrix4fv(uView, false, view);
      gl.uniform3fv(uEye, eye);
      gl.uniform1f(uExposure, settings.exposure);
      gl.uniform1f(uSizeScale, settings.sizeScale);
      gl.uniform1f(uMaxRadius, settings.maxRadius);
      gl.uniform1f(uMaxPoint, maxPointSize);
      gl.uniform1f(uStandMask, 0);
      gl.uniform1f(uStandBoost, 1);
      gl.enable(gl.BLEND);
      gl.blendFunc(gl.ONE, gl.ONE);
      gl.disableVertexAttribArray(QUAD);
      for (const slot of [POSITION, LUMINOSITY, BP_RP]) gl.enableVertexAttribArray(slot);
    },

    draw(ranges, stands) {
      let bound = -1;
      let standing = false;
      for (let i = 0, j = 0; i < ranges.length; i += 3, j += STAND_FLOATS) {
        if (ranges[i] !== bound) {
          bound = ranges[i];
          bind(bound);
        }
        // Most spans are ordinary; the mask is only touched around the few
        // that are not, so a fully loaded view sets no uniform per draw.
        const mask = stands ? stands[j + 4] : 0;
        if (mask !== 0) {
          gl.uniform3f(uStandCenter, stands![j], stands![j + 1], stands![j + 2]);
          gl.uniform1f(uStandBoost, stands![j + 3]);
          gl.uniform1f(uStandMask, mask);
          standing = true;
        } else if (standing) {
          gl.uniform1f(uStandMask, 0);
          gl.uniform1f(uStandBoost, 1);
          standing = false;
        }
        gl.drawArrays(gl.POINTS, ranges[i + 1], ranges[i + 2]);
      }
    },

    end() {
      gl.disable(gl.BLEND);
      gl.bindFramebuffer(gl.FRAMEBUFFER, null);
      gl.useProgram(tonemap);
      for (const slot of [POSITION, LUMINOSITY, BP_RP]) gl.disableVertexAttribArray(slot);
      gl.enableVertexAttribArray(QUAD);
      gl.bindBuffer(gl.ARRAY_BUFFER, quad);
      gl.vertexAttribPointer(QUAD, 2, gl.FLOAT, false, 0, 0);
      gl.bindTexture(gl.TEXTURE_2D, hdr);
      gl.drawArrays(gl.TRIANGLES, 0, 3);
    },

    render(projection, view, eye, ranges, settings, stands) {
      this.begin(projection, view, eye, settings);
      this.draw(ranges, stands);
      this.end();
    },
  };
}
