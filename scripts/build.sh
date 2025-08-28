#!/bin/bash

# Build script for File Server
# Usage: ./build.sh [debug|release] [clean]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BUILD_TYPE="Release"
CLEAN=false
TESTS=true
EXAMPLES=true
COVERAGE=false
LTO=false
OPTIMIZATIONS=false

# Function to print colored output
print_color() {
    echo -e "${1}${2}${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -t, --type TYPE      Build type (Debug|Release) [default: Release]"
    echo "  -c, --clean          Clean build directory before building"
    echo "  --no-tests           Don't build tests"
    echo "  --no-examples        Don't build examples"
    echo "  --coverage           Enable code coverage (Debug builds only)"
    echo "  --lto                Enable Link Time Optimization"
    echo "  --optimize           Enable additional optimizations"
    echo "  -j, --jobs N         Number of parallel jobs [default: auto]"
    echo "  -h, --help           Show this help message"
}

# Parse command line arguments
JOBS=$(nproc)
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        -c|--clean)
            CLEAN=true
            shift
            ;;
        --no-tests)
            TESTS=false
            shift
            ;;
        --no-examples)
            EXAMPLES=false
            shift
            ;;
        --coverage)
            COVERAGE=true
            shift
            ;;
        --lto)
            LTO=true
            shift
            ;;
        --optimize)
            OPTIMIZATIONS=true
            shift
            ;;
        -j|--jobs)
            JOBS="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        debug|release)
            BUILD_TYPE="$(tr '[:lower:]' '[:upper:]' <<< ${1:0:1})${1:1}"
            shift
            ;;
        clean)
            CLEAN=true
            shift
            ;;
        *)
            print_color $RED "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate build type
if [[ "$BUILD_TYPE" != "Debug" && "$BUILD_TYPE" != "Release" ]]; then
    print_color $RED "Invalid build type: $BUILD_TYPE. Must be Debug or Release."
    exit 1
fi

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build-$(tr '[:upper:]' '[:lower:]' <<< "$BUILD_TYPE")"

print_color $BLUE "File Server Build Script"
print_color $BLUE "======================="
echo "Build type:    $BUILD_TYPE"
echo "Build dir:     $BUILD_DIR"
echo "Clean build:   $CLEAN"
echo "Tests:         $TESTS"
echo "Examples:      $EXAMPLES"
echo "Coverage:      $COVERAGE"
echo "LTO:           $LTO"
echo "Optimizations: $OPTIMIZATIONS"
echo "Jobs:          $JOBS"
echo

# Check for required tools
print_color $YELLOW "Checking build requirements..."
command -v cmake >/dev/null 2>&1 || { print_color $RED "cmake is required but not installed. Aborting."; exit 1; }
command -v git >/dev/null 2>&1 || { print_color $RED "git is required but not installed. Aborting."; exit 1; }

# Check for vcpkg
if [[ -z "$VCPKG_ROOT" ]]; then
    print_color $YELLOW "VCPKG_ROOT not set. Looking for vcpkg in common locations..."
    if [[ -d "/opt/vcpkg" ]]; then
        export VCPKG_ROOT="/opt/vcpkg"
    elif [[ -d "$HOME/vcpkg" ]]; then
        export VCPKG_ROOT="$HOME/vcpkg"
    elif [[ -d "$PROJECT_DIR/vcpkg" ]]; then
        export VCPKG_ROOT="$PROJECT_DIR/vcpkg"
    else
        print_color $RED "vcpkg not found. Please install vcpkg and set VCPKG_ROOT environment variable."
        exit 1
    fi
fi

print_color $GREEN "Using vcpkg from: $VCPKG_ROOT"

# Clean build directory if requested
if [[ "$CLEAN" == true ]]; then
    print_color $YELLOW "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure CMake options
CMAKE_OPTIONS=(
    "-DCMAKE_BUILD_TYPE=$BUILD_TYPE"
    "-DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
    "-DBUILD_TESTS=$TESTS"
    "-DBUILD_EXAMPLES=$EXAMPLES"
    "-DENABLE_COVERAGE=$COVERAGE"
    "-DENABLE_LTO=$LTO"
    "-DENABLE_OPTIMIZATIONS=$OPTIMIZATIONS"
)

# Configure project
print_color $YELLOW "Configuring project..."
cmake "${CMAKE_OPTIONS[@]}" "$PROJECT_DIR"

# Build project
print_color $YELLOW "Building project..."
cmake --build . --config "$BUILD_TYPE" -j "$JOBS"

# Run tests if enabled
if [[ "$TESTS" == true ]]; then
    print_color $YELLOW "Running tests..."
    ctest --output-on-failure --parallel "$JOBS"
fi

print_color $GREEN "Build completed successfully!"
print_color $BLUE "Binary location: $BUILD_DIR/bin/file_server"

# Show next steps
echo
print_color $BLUE "Next steps:"
echo "1. Configure server: edit config/server.json"
echo "2. Generate SSL certificates: ./scripts/generate_ssl.sh"
echo "3. Run server: $BUILD_DIR/bin/file_server --config config/server.json"

if [[ "$COVERAGE" == true && "$BUILD_TYPE" == "Debug" ]]; then
    echo "4. Generate coverage report: make coverage"
fi