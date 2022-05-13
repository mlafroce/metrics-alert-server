FROM rust:latest AS builder
LABEL intermediateStageToBeDeleted=true

COPY metric-alert-service build
RUN cd build && cargo build --release

FROM ubuntu:latest
COPY --from=builder /build/target/release/server /usr/bin/server
ENTRYPOINT ["server"]

