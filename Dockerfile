FROM rust:1-slim AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12
COPY --from=build /app/target/release/screenshare /screenshare
ENV BIND=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/screenshare"]
