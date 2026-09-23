{
  lib,
  buildNpmPackage,

  version,
}:

buildNpmPackage {
  pname = "llama-swap-ui";
  inherit version;

  # ui/ is self-contained: package-lock.json, .npmrc and vite.config.ts all
  # live here, so the derivation takes ui/ as its root instead of the repo
  # root. A change under internal/ then does not invalidate the UI build.
  src = lib.fileset.toSource {
    root = ../ui;
    fileset = lib.fileset.unions [
      ../ui/src
      ../ui/public
      ../ui/.npmrc
      ../ui/components.json
      ../ui/index.html
      ../ui/package-lock.json
      ../ui/package.json
      ../ui/svelte.config.js
      ../ui/tsconfig.json
      ../ui/vite.config.ts
    ];
  };

  # Run `nix build .#llama-swap-ui` after changing ui/package-lock.json and
  # copy the hash nix reports into this attribute.
  npmDepsHash = "sha256-lmhRJ8275PIQ+7vHdr9aZ31lYeXUkXrWnlvuwOadjRQ=";

  # `make ui` writes the bundle straight into the Go tree so that the
  # `//go:embed all:ui_dist` in internal/server/embed.go can pick it up. A nix
  # derivation may only write into its own $out, so point vite there instead;
  # nix/package.nix copies $out/ui_dist into the Go tree in its preBuild.
  postPatch = ''
    substituteInPlace vite.config.ts \
      --replace-fail '"../internal/server/ui_dist"' '"${placeholder "out"}/ui_dist"'
  '';

  # ui/.npmrc sets legacy-peer-deps=true; npm reads it from the source tree,
  # but state it here too so the flag survives if that file ever moves.
  npmFlags = [ "--legacy-peer-deps" ];

  # The default install also stages node_modules under $out/lib. Only ui_dist
  # is consumed downstream.
  postInstall = ''
    rm -rf "$out/lib"
  '';

  meta = {
    description = "Web UI embedded into the llama-swap binary";
    homepage = "https://github.com/mostlygeek/llama-swap";
    license = lib.licenses.mit;
  };
}
