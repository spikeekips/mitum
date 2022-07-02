#!/bin/sh

l=$(go list -tags 'test' -json ./... | jq -r -c '. | .Dir' | sed -e "s@$(pwd)@@g")
if [ -z "$l" ];then
    l="."
fi

for i in $l
do
    echo $i
    i=$(echo $i | sed -e 's@^/@@g')

    ts=$(find $i -maxdepth 1 -name '*.go' -exec grep 'func Test.*' {} \; | sed -e 's/.*func //g' -e 's/(.*//g' | sort)
    for t in $ts
    do
        time go test -race -coverprofile=/tmp/cov.out -tags 'test redis' -v -timeout 20m -failfast ./$i -run "^$t$"
        e=$?
        if [ $e -ne 0 ];then
            exit $e
        fi

        cat /tmp/cov.out >> /tmp/allcov.out
    done
done
