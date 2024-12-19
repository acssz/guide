{
  description = "ACSSZ guide for new students and scholars";

  # TODO 使用上游仓库
  inputs.nixpkgs.url = "github:yoctocell/nixpkgs/add-python-lark-oapi";

  outputs = { self, nixpkgs }:
    let pkgs = nixpkgs.legacyPackages.x86_64-linux;
    in with pkgs; {
      devShell.x86_64-linux = mkShell {
        name = "acssz-guide";
        buildInputs = [
          # For compressing pdfs, see <https://askubuntu.com/a/256449>
          ghostscript
          pkgs.python3Packages.pymupdf
          (pkgs.python3.withPackages (pp: with pp; [
            requests
            websockets
            lark-oapi
          ]))
        ];
      };
   };
}
