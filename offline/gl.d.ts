declare module "gl" {
  function createContext(
    width: number, height: number, options?: WebGLContextAttributes,
  ): WebGLRenderingContext;
  export = createContext;
}
