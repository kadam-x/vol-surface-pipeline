{
  description = "python project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python311
            uv
            stdenv.cc.cc.lib
            zlib
          ];

          shellHook = ''
            export UV_PROJECT_ENVIRONMENT="$PWD/.venv"
            export VIRTUAL_ENV="$PWD/.venv"
            if [ ! -d ".venv" ]; then
              uv venv --python ${pkgs.python311}/bin/python3.11
            fi
            source .venv/bin/activate
            export LD_LIBRARY_PATH="${
              pkgs.lib.makeLibraryPath [
                pkgs.stdenv.cc.cc.lib
                pkgs.zlib
              ]
            }:$LD_LIBRARY_PATH"
          '';
        };
      }
    );
}
