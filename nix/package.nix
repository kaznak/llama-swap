{
  lib,
  stdenv,

  # go.mod asks for go 1.27.1; the default buildGoModule may still be on 1.26.
  buildGo127Module,
  callPackage,

  versionCheckHook,

  # Build metadata stamped into the binary. `make` fills these from git (see
  # GIT_VERSION / GIT_HASH / BUILD_DATE in the Makefile); a nix build has no
  # git in its sandbox, so they are passed in instead. flake.nix derives them
  # from the revision it is building. A plain
  # `pkgs.callPackage ./nix/package.nix { }` gets the defaults below, and
  # `llama-swap -version` then reports the commit as "unknown".
  #
  # Not named `commit`: callPackage fills any argument that matches an
  # attribute of the package set, and nixpkgs has a package called `commit`,
  # so the default would silently become a store path.
  commitHash ? "unknown",
  buildDate ? "1970-01-01T00:00:00Z",

  # Build the Svelte UI and embed it. Turning this off drops the node
  # toolchain from the build closure; internal/server/embed_notag.go then
  # serves an empty filesystem for /ui.
  withUI ? true,
}:

let
  # Bumped by `make release` together with the git tag. Keep in sync with the
  # newest heading in CHANGELOG.md.
  version = "257";

  ui = callPackage ./ui.nix { inherit version; };

  # Cross builds cannot run the binaries they produce.
  canExecute = stdenv.buildPlatform.canExecute stdenv.hostPlatform;
in
buildGo127Module {
  pname = "llama-swap";
  inherit version;

  src = lib.fileset.toSource {
    root = ../.;
    # ui/ is built by nix/ui.nix and pulled in via preBuild, so leaving it out
    # here keeps a UI-only change from rebuilding the Go package from scratch.
    fileset = lib.fileset.unions [
      ../cmd
      ../docs
      ../internal
      ../config-schema.json # //go:embed in llama-swap.go
      ../config.example.yaml # installed to share/, see postInstall
      ../go.mod
      ../go.sum
      ../llama-swap.go
      ../llama-swap_test.go
    ];
  };

  # go.mod requires golang.org/x/sync twice: once directly (line 22) and once
  # more in the "// indirect" block. `go build` with the module cache does not
  # care, but nix builds from a vendor directory, and `go mod vendor` collapses
  # the two into one entry in vendor/modules.txt. The vendor consistency check
  # then finds 160 requirements against 159 explicit modules and aborts with
  # "updates to go.mod needed, disabled by -mod=vendor".
  #
  # Drop the duplicate here rather than in go.mod so that this branch adds
  # nothing outside nix/ and flake.nix. `go mod tidy` removes exactly this
  # line, so this patch can go away as soon as go.mod is tidied upstream.
  postPatch = ''
    grep -q '^	golang.org/x/sync v[0-9.]* // indirect$' go.mod
    sed -i '/^	golang.org\/x\/sync v[0-9.]* \/\/ indirect$/d' go.mod
  '';

  # Run `nix build` after changing go.mod/go.sum and copy the hash nix reports
  # into this attribute.
  vendorHash = "sha256-QidyJnXP4w9yKm2GckaEl4QJZm5vikPYkD4fiysb9x0=";

  # Only these two are end-user binaries. cmd/{fake-model,misc,monitor-test,
  # test-concurrency} are development/benchmark helpers, cmd/simple-responder
  # is a test fixture, and cmd/{kubeswap,vllm-wrapper} are deployment wrappers
  # with their own release story (see the Makefile's kubeswap target).
  subPackages = [
    "."
    "cmd/wol-proxy"
  ];

  # internal/server/embed.go is only compiled with this tag set.
  tags = lib.optionals withUI [ "embed_ui" ];

  ldflags = [
    "-s"
    "-w"
    "-X main.version=${version}"
    "-X main.commit=${commitHash}"
    "-X main.date=${buildDate}"
  ];

  preBuild = lib.optionalString withUI ''
    # Satisfies the //go:embed all:ui_dist in internal/server/embed.go.
    cp -r ${ui}/ui_dist internal/server/
  '';

  # `subPackages` narrows the check phase to the two main packages, which have
  # almost no tests of their own, and the suites under internal/ spawn real
  # processes and bind ports. Run those with `make test-all` instead of in the
  # build sandbox.
  doCheck = false;

  nativeInstallCheckInputs = [ versionCheckHook ];
  doInstallCheck = canExecute;
  versionCheckProgramArg = "-version";

  postInstall = ''
    install -Dm444 -t "$out/share/llama-swap" config.example.yaml
  '';

  passthru = {
    inherit ui;
  };

  __structuredAttrs = true;

  meta = {
    homepage = "https://github.com/mostlygeek/llama-swap";
    changelog = "https://github.com/mostlygeek/llama-swap/releases/tag/v${version}";
    description = "Model swapping for llama.cpp (or any local OpenAPI compatible server)";
    license = lib.licenses.mit;
    mainProgram = "llama-swap";
    platforms = lib.platforms.unix;
  };
}
