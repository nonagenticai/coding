#!/usr/bin/env bash
# Build the `coding` client on the build host and install it here.
#
# The estate forbids cargo on mini3 inside a Cargo project — the shim refuses — so the binary is
# built through rbuild on the build host and copied back. The script proves the install by
# executing the result: a build script that reports success without running what it produced is
# exactly the kind of gate that cannot fail.
#
#   scripts/install-client.sh              # -> ~/.local/bin/coding
#   scripts/install-client.sh /some/path   # -> /some/path
#
# The build hosts are darwin/arm64 like this box, so the artifact runs here unchanged.
set -euo pipefail

DEST="${1:-$HOME/.local/bin/coding}"
HOST="${RBUILD_HOST:-m1}"

root=$(cd "$(dirname "$0")/.." && pwd -P)
# rbuild names its remote build slot after the project directory's basename.
repo=$(basename "$root")

cd "$root"
echo "==> building coding on $HOST"
rbuild -- build --release --locked --bin coding

echo "==> locating the artifact on $HOST"
# rbuild keeps slot 0 in <root>/{src,target} and slot N in <root>/s<N>/{src,target}: `target` is a
# SIBLING of `src`, never a child of it.
artifact=$(ssh -o BatchMode=yes "$HOST" \
    "ls -t ~/build/$repo/target/release/coding ~/build/$repo/s*/target/release/coding 2>/dev/null | head -1")
if [ -z "$artifact" ]; then
    echo "ERROR: no target/release/coding under ~/build/$repo on $HOST." >&2
    echo "       Refusing to guess where it went; check 'rbuild --status'." >&2
    exit 1
fi

echo "==> copying $HOST:$artifact -> $DEST"
mkdir -p "$(dirname "$DEST")"
ssh -o BatchMode=yes "$HOST" "cat '$artifact'" >"$DEST.tmp"
chmod 0755 "$DEST.tmp"
mv "$DEST.tmp" "$DEST"

# Proof, not a claim: the installed binary answers on this machine.
"$DEST" --version
echo "installed $("$DEST" --version) at $DEST"
