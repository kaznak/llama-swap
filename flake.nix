{
  description = "Model swapping for llama.cpp (or any local OpenAPI compatible server)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;

      # The same set the Makefile's release targets cover, minus Windows:
      # aarch64-linux for ARM servers, aarch64-darwin for Apple silicon
      # (`make mac` builds darwin/arm64 only). Nothing here is
      # architecture-specific - it is Go plus an embedded static bundle - but
      # only x86_64-linux has actually been built so far.
      #
      # x86_64-darwin is deliberately absent: nixpkgs 26.11 dropped it.
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # -- build metadata -------------------------------------------------
      #
      # llama-swap stamps the git commit and the build date into the binary at
      # link time (`-X main.commit` / `-X main.date`, see the Makefile) and
      # serves them from /api/version. A build without those ldflags reports
      # the placeholder values compiled into llama-swap.go: version "0",
      # commit "abcd1234", date "unknown". Anything that records which binary
      # produced a log would lose the provenance, so the flake always supplies
      # them from its own source metadata rather than leaving them unset.
      #
      # git is not available inside the build sandbox, so the values come from
      # the flake's view of the tree:
      #
      #   clean checkout   self.shortRev       e.g. "d1c6429"
      #   dirty checkout   self.dirtyShortRev  e.g. "d1c6429-dirty"
      #   tarball / no git neither attribute exists -> "unknown"
      #
      # In both git cases self.lastModifiedDate is the committer date of HEAD,
      # so a dirty build reports the date of the commit it was based on.
      #
      # A dirty tree reports "<short>-dirty", the same signal as the
      # Makefile's "<short>+" marker: the binary does not correspond to any
      # published commit. Measured on a dirty tree at commit d1c6429:
      #
      #   $ ./result/bin/llama-swap -version
      #   version: 257 (d1c6429-dirty), built at 2026-09-22T17:14:16Z
      commit = self.dirtyShortRev or self.shortRev or "unknown";

      # self.lastModifiedDate is "YYYYMMDDhhmmss" in UTC. main.date is printed
      # verbatim, and the Makefile puts RFC3339 there, so convert.
      date =
        let
          d = self.lastModifiedDate or "19700101000000";
          part = start: len: builtins.substring start len d;
        in
        "${part 0 4}-${part 4 2}-${part 6 2}T${part 8 2}:${part 10 2}:${part 12 2}Z";
    in
    {
      packages = forAllSystems (pkgs: rec {
        default = llama-swap;
        llama-swap = pkgs.callPackage ./nix/package.nix {
          commitHash = commit;
          buildDate = date;
        };
        # The Svelte bundle on its own, without the Go binary around it.
        llama-swap-ui = llama-swap.ui;
      });

      # For flake consumers that already have their own nixpkgs:
      #   nixpkgs.overlays = [ llama-swap.overlays.default ];
      overlays.default = final: _prev: {
        llama-swap = final.callPackage ./nix/package.nix {
          commitHash = commit;
          buildDate = date;
        };
      };

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go_1_27
            pkgs.gnumake
            pkgs.nodejs
          ];
        };
      });

      checks = forAllSystems (pkgs: {
        inherit (self.packages.${pkgs.stdenv.hostPlatform.system}) llama-swap llama-swap-ui;
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt-tree);
    };
}
