{
  description = "credible-sdk flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        llvm = pkgs.llvmPackages_latest;
        clangStdenv = llvm.stdenv;

        rustNightly = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "cargo" ];
        };

        systemSpecific = with pkgs;
          if stdenv.isDarwin then [
            darwin.apple_sdk.frameworks.Security
            darwin.apple_sdk.frameworks.SystemConfiguration
            libiconv
          ] else [
            llvm.libcxx
          ];

      in
      {
        devShells.default = pkgs.mkShell {
          # Use the clang stdenv to set default CC/CXX etc.
          stdenv = clangStdenv;
          name = "op-talos-dev";

          buildInputs = with pkgs; [
            rustNightly
            cargo-flamegraph
            cargo-fuzz

            # Explicitly list llvm tools for clarity and PATH access
            llvm.clang
            llvm.lldb
            llvm.libclang

            gnumake
            pkg-config
            openssl
            git

          ] ++ systemSpecific;

          shellHook = ''
            export OLD_PS1="$PS1"
            export PS1="nix-shell:credible-sdk $PS1"

            export RUST_BACKTRACE=1
            export RUST_SRC_PATH="${rustNightly}/lib/rustlib/src/rust/library"

            # Explicitly set CC/CXX even though stdenv does it, for certainty
            export CC="${llvm.clang}/bin/clang"
            export CXX="${llvm.clang}/bin/clang++"

            # For jemalloc
            export CFLAGS="-O3 $CFLAGS"
            export CXXFLAGS="-O3 $CXXFLAGS"

            export LIBCLANG_PATH="${llvm.libclang.lib}/lib"

            export PATH=$HOME/.cargo/bin:$PATH

            rustup default nightly 2>/dev/null || true

            ${if pkgs.stdenv.isDarwin then ''
              export DYLD_LIBRARY_PATH="${pkgs.openssl.out}/lib:$DYLD_LIBRARY_PATH"
              export LIBRARY_PATH="${pkgs.openssl.out}/lib:$LIBRARY_PATH"
              export CFLAGS="-I${pkgs.openssl.dev}/include $CFLAGS"
              export CXXFLAGS="-I${pkgs.openssl.dev}/include $CXXFLAGS"
              export LDFLAGS="-L${pkgs.openssl.out}/lib $LDFLAGS"
            '' else ''
              export LD_LIBRARY_PATH="${pkgs.openssl.out}/lib:$LD_LIBRARY_PATH"
              export CFLAGS="-I${pkgs.openssl.dev}/include $CFLAGS"
              export CXXFLAGS="-I${pkgs.openssl.dev}/include $CXXFLAGS"
              export LDFLAGS="-L${pkgs.openssl.out}/lib $LDFLAGS"
            ''}
          '';

          shellExitHook = ''
            export PS1="$OLD_PS1"
          '';
        };
      });
}