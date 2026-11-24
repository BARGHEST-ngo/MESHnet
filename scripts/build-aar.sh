#!/usr/bin/env bash
# Builds the MESHnet Android AAR (package meshnetmobile) with gomobile.
# Env overrides:
#   ANDROID_HOME   Android SDK root (required)
#   NDK_VERSION    NDK to build with (default: r28: 16 KB page alignment required for API 35+ targets; takes precedence over ANDROID_NDK_HOME)
#   ANDROID_API    minimum API level (default: 26)
#   TARGET         gomobile target (default: android - all architectures)
set -euo pipefail

NDK_VERSION="${NDK_VERSION:-28.2.13676358}"
ANDROID_API="${ANDROID_API:-26}"
TARGET="${TARGET:-android}"
OUT="build/meshnet.aar"

cd "$(dirname "$0")/.."
: "${ANDROID_HOME:?ANDROID_HOME must point at the Android SDK}"

# Prefer the pinned NDK over any inherited ANDROID_NDK_HOME:
# CI runners preset it to their default NDK, which silently overrode NDK_VERSION.
if [[ -d "$ANDROID_HOME/ndk/$NDK_VERSION" ]]; then
	export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/$NDK_VERSION"
fi
: "${ANDROID_NDK_HOME:?NDK $NDK_VERSION not found (install ndk;$NDK_VERSION or set ANDROID_NDK_HOME)}"
ndk_rev="$(sed -n 's/^Pkg.Revision *= *//p' "$ANDROID_NDK_HOME/source.properties" 2>/dev/null)"

if [[ "$ndk_rev" != "$NDK_VERSION" ]]; then
	echo "NDK at $ANDROID_NDK_HOME is ${ndk_rev:-unknown}, expected $NDK_VERSION" >&2
	exit 1
fi

mkdir -p build
TOOLS_BIN="$PWD/build/bin"
GOBIN="$TOOLS_BIN" go install golang.org/x/mobile/cmd/gomobile golang.org/x/mobile/cmd/gobind
export PATH="$TOOLS_BIN:$PATH"
gomobile bind \
	-target "$TARGET" \
	-androidapi "$ANDROID_API" \
	-javapkg org.barghest.meshnet \
	-trimpath \
	-ldflags "-s -w" \
	-o "$OUT" \
	./mobile

# Google Play requires 16 KB-aligned LOAD segments on 64-bit ABIs (API 35+ targets).
readelf="$(echo "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin/llvm-readelf)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
unzip -q "$OUT" 'jni/*' -d "$tmp"
shopt -s nullglob
for so in "$tmp"/jni/{arm64-v8a,x86_64}/*.so; do
	align="$("$readelf" -lW "$so" | awk '/LOAD/ {print $NF}' | sort -u)"
	if [[ "$align" != "0x4000" ]]; then
		echo "$(basename "$(dirname "$so")")/$(basename "$so") is not 16 KB aligned (LOAD align: ${align:-none})" >&2
		exit 1
	fi
done

echo "Built $OUT ($TARGET, API $ANDROID_API, NDK $(basename "$ANDROID_NDK_HOME"))"
sha256sum "$OUT"