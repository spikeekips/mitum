#!/bin/bash

set -e

GOVERSION=$(go version | cut -d " " -f 3 | cut -b 3-6)

supported=( "android/arm64" "darwin/amd64" "darwin/arm64" "dragonfly/amd64" "freebsd/386" "freebsd/arm" "freebsd/arm64" "freebsd/amd64" "illumos/amd64" "linux/386" "linux/amd64" "linux/arm" "linux/arm64" "linux/mips" "linux/mips64" "linux/mips64le" "linux/mipsle" "linux/ppc64" "linux/ppc64le" "linux/riscv64" "linux/s390x" "netbsd/386" "netbsd/amd64" "netbsd/arm" "netbsd/arm64" "openbsd/386" "openbsd/amd64" "openbsd/arm" "openbsd/arm64" "solaris/amd64" "windows/386" "windows/amd64" "windows/arm" "windows/arm64" )

races=( "linux/amd64" )

for dist in ${supported[@]}; do
	goos=$(echo $dist | cut -d "/" -f1)
	goarch=$(echo $dist | cut -d "/" -f2)

	echo "$dist"

    b=/tmp/main-${goos}-${goarch}
	GOOS=$goos GOARCH=$goarch go build -o $b example/*.go

	rm $b
done

for dist in ${races[@]}; do
	goos=$(echo $dist | cut -d "/" -f1)
	goarch=$(echo $dist | cut -d "/" -f2)

	echo "$dist"

    b=/tmp/main-${goos}-${goarch}
	GOOS=$goos GOARCH=$goarch go build -race -o $b example/*.go

	rm $b
done
