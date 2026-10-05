#!/usr/bin/env bash
# Builds the MESHnet Android AAR (package meshnetmobile) with gomobile.
# Env overrides:
#   ANDROID_HOME   Android SDK root (required)
#   NDK_VERSION    NDK to build with (default: matches MESH build.gradle)
#   ANDROID_API    minimum API level (default: 26, MESH minSdkVersion)
#   TARGET         gomobile target (default: android - all architectures)
set -euo pipefail

NDK_VERSION="${NDK_VERSION:-23.2.8568313}"
ANDROID_API="${ANDROID_API:-26}"
TARGET="${TARGET:-android}"
OUT="build/meshnet.aar"

cd "$(dirname "$0")/.."
: "${ANDROID_HOME:?ANDROID_HOME must point at the Android SDK}"

# gomobile picks the highest installed NDK unless told otherwise.
export ANDROID_NDK_HOME="${ANDROID_NDK_HOME:-$ANDROID_HOME/ndk/$NDK_VERSION}"
if [[ ! -d "$ANDROID_NDK_HOME" ]]; then
	echo "NDK not found at $ANDROID_NDK_HOME (install ndk;$NDK_VERSION or set ANDROID_NDK_HOME)" >&2
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

echo "Built $OUT ($TARGET, API $ANDROID_API, NDK $(basename "$ANDROID_NDK_HOME"))"
sha256sum "$OUT"