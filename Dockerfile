from mcluseau/golang-builder:1.21.6 as build

from alpine:3.19
run apk add --no-cache nftables
entrypoint ["/bin/knl-nft"]
copy --from=build /go/bin/ /bin/
