#!/usr/bin/env bash
# Fetch, build and install the clink this project runs against.
#
# card-sentry consumes clink the way any downstream project does: install a
# release into a local prefix and build against the installed CMake package
# and CLI. Everything lands under .clink/ inside this repository; nothing is
# written to system paths.
#
# Two modes:
#
#   Release (default)   clone the pinned tag, build, install. This is the
#                       mode the published repository ships with.
#
#   Local source        CLINK_SOURCE=/path/to/clink installs that working
#                       tree into the same prefix layout instead. Used while
#                       card-sentry is developed against a moving engine; the
#                       consumption seam (installed package + CLI) stays
#                       identical, so flipping back to a release changes
#                       nothing else in this repository. The install stamp
#                       records the source commit (and a -dirty marker), and
#                       a dirty tree always reinstalls.
#
#   CLINK_VERSION      release tag to pin (default: the version this repo ships against)
#   CLINK_REPO         git URL (default: https://github.com/orhaugh/clink)
#   CLINK_SOURCE      path to a local clink checkout; overrides the tag path
#   CLINK_JOBS         build parallelism (default: number of CPUs, capped at 10)
#   CLINK_SKIP_ICEBERG set to 1 to skip the iceberg-cpp toolchain build and the
#                      Iceberg impl. Required when the pinned Arrow is built
#                      without object stores (CLINK_ARROW_OBJECT_STORES=OFF, as
#                      CI does): iceberg-cpp's bundle references Arrow's S3
#                      symbols unconditionally, so it cannot link against a
#                      no-S3 Arrow. Nothing here uses Iceberg.
#   CLINK_CMAKE_ARGS   extra arguments appended to the clink configure line
#
# Idempotent: re-running against an existing matching install is a no-op.

set -euo pipefail

CLINK_VERSION="${CLINK_VERSION:-v0.5.0}"
CLINK_REPO="${CLINK_REPO:-https://github.com/orhaugh/clink}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${ROOT}/.clink/src"
PREFIX="${ROOT}/.clink/prefix"
STAMP="${PREFIX}/.card-sentry-installed"

ncpu="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
JOBS="${CLINK_JOBS:-$(( ncpu < 10 ? ncpu : 10 ))}"

configure_build_install() {
    # $1 = source dir, $2 = build dir, $3 = stamp contents
    local src="$1" bld="$2" stamp="$3"
    (cd "${src}" && scripts/build-arrow.sh)
    local extra_args=()
    if [[ "${CLINK_SKIP_ICEBERG:-0}" == "1" ]]; then
        extra_args+=("-DCLINK_WITH_ICEBERG=OFF")
    else
        (cd "${src}" && scripts/build-iceberg-cpp.sh)
    fi
    if [[ -n "${CLINK_CMAKE_ARGS:-}" ]]; then
        # Deliberate word-splitting: CLINK_CMAKE_ARGS is a flat argument string.
        # shellcheck disable=SC2206
        extra_args+=(${CLINK_CMAKE_ARGS})
    fi
    # ${arr[@]+...} keeps the empty-array expansion safe under `set -u`
    # on bash 3.2 (the macOS system bash).
    cmake -S "${src}" -B "${bld}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCLINK_BUILD_SQL=ON \
        -DCLINK_BUILD_TESTS=OFF \
        -DCLINK_BUILD_EXAMPLES=OFF \
        -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
        ${extra_args[@]+"${extra_args[@]}"}
    cmake --build "${bld}" --parallel "${JOBS}"
    cmake --install "${bld}"
    echo "${stamp}" > "${STAMP}"
    echo
    echo "== clink (${stamp}) installed."
    echo "   CLI:            ${PREFIX}/bin/clink"
    echo "   CMake package:  ${PREFIX}/lib/cmake/clink (use -DCMAKE_PREFIX_PATH=${PREFIX})"
}

# ---- Local-source mode ------------------------------------------------------
if [[ -n "${CLINK_SOURCE:-}" ]]; then
    SRC_DIR="$(cd "${CLINK_SOURCE}" && pwd)"
    if [[ ! -f "${SRC_DIR}/CMakeLists.txt" ]]; then
        echo "CLINK_SOURCE=${SRC_DIR} does not look like a clink checkout" >&2
        exit 2
    fi
    sha="$(git -C "${SRC_DIR}" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
    dirty=""
    if ! git -C "${SRC_DIR}" diff --quiet HEAD 2>/dev/null; then
        dirty="-dirty"
    fi
    want="source:${sha}${dirty}"
    # A clean tree at the stamped commit is a no-op; a dirty tree always
    # reinstalls (the build is incremental, so this is cheap).
    if [[ -z "${dirty}" && -x "${PREFIX}/bin/clink" && -f "${STAMP}" ]] \
       && grep -qx "${want}" "${STAMP}"; then
        echo "clink ${want} already installed at ${PREFIX}"
        exit 0
    fi
    echo "== card-sentry: installing clink from ${SRC_DIR} (${want})"
    configure_build_install "${SRC_DIR}" "${ROOT}/.clink/source-build" "${want}"
    exit 0
fi

# ---- Release mode -----------------------------------------------------------
if [[ -x "${PREFIX}/bin/clink" && -f "${STAMP}" ]] \
   && grep -qx "${CLINK_VERSION}" "${STAMP}"; then
    echo "clink ${CLINK_VERSION} already installed at ${PREFIX}"
    exit 0
fi

echo "== card-sentry: installing clink ${CLINK_VERSION} into ${PREFIX}"

# Fast path: a prebuilt SDK tarball on the release (published from v0.4.0
# onwards for linux x86_64, glibc floor Ubuntu 24.04). Download, verify the
# published checksum, extract - about a minute, no compiler involved. Any
# miss (other platform, older release, no network) falls through to the
# source build below. CLINK_FROM_SOURCE=1 skips the attempt entirely.
if [[ "${CLINK_FROM_SOURCE:-0}" != "1" && "$(uname -s)" == "Linux" && "$(uname -m)" == "x86_64" ]]; then
    asset="clink-${CLINK_VERSION}-linux-x86_64-ubuntu24.04.tar.gz"
    base="${CLINK_REPO%.git}/releases/download/${CLINK_VERSION}"
    tmp="$(mktemp -d)"
    echo "-- trying the prebuilt SDK: ${base}/${asset}"
    if curl -fsSL -o "${tmp}/${asset}" "${base}/${asset}" \
       && curl -fsSL -o "${tmp}/${asset}.sha256" "${base}/${asset}.sha256"; then
        ( cd "${tmp}" && sha256sum -c "${asset}.sha256" >/dev/null )
        mkdir -p "${ROOT}/.clink"
        rm -rf "${PREFIX}"
        tar xzf "${tmp}/${asset}" -C "${tmp}"
        mv "${tmp}/${asset%.tar.gz}" "${PREFIX}"
        rm -rf "${tmp}"
        echo "${CLINK_VERSION}" > "${STAMP}"
        echo "== clink ${CLINK_VERSION} installed from the prebuilt SDK."
        echo "   CLI:            ${PREFIX}/bin/clink"
        echo "   CMake package:  ${PREFIX}/lib/cmake/clink (use -DCMAKE_PREFIX_PATH=${PREFIX})"
        exit 0
    fi
    rm -rf "${tmp}"
    echo "-- no prebuilt SDK for this release/platform; building from source"
fi

# 1. The release source, shallow, at the pinned tag.
if [[ -d "${SRC}/.git" ]]; then
    have="$(git -C "${SRC}" describe --tags --exact-match 2>/dev/null || true)"
    if [[ "${have}" != "${CLINK_VERSION}" ]]; then
        echo "-- existing checkout is '${have:-unknown}', refetching ${CLINK_VERSION}"
        rm -rf "${SRC}"
    fi
fi
if [[ ! -d "${SRC}/.git" ]]; then
    git clone --depth 1 --branch "${CLINK_VERSION}" "${CLINK_REPO}" "${SRC}"
fi

# 2 + 3. clink's pinned Arrow/Parquet/iceberg toolchain (~/.clink-deps by
# default; fast on platforms with a prebuilt archive), then build the engine
# with the SQL frontend and install it locally.
configure_build_install "${SRC}" "${SRC}/build" "${CLINK_VERSION}"
