# ── Build stage ──────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/sdk:8.0-alpine AS build
WORKDIR /src

COPY ElevenLabsProxy.csproj ./
RUN dotnet restore

COPY *.cs ./
RUN dotnet publish -c Release -o /app --no-restore

# ── Runtime stage ────────────────────────────────────────────────────────
FROM mcr.microsoft.com/dotnet/aspnet:8.0-alpine AS runtime
WORKDIR /app

RUN addgroup -S proxy && adduser -S proxy -G proxy

COPY --from=build /app ./

USER proxy

EXPOSE 3000

ENV ASPNETCORE_URLS=http://+:3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3000/ready || exit 1

ENTRYPOINT ["dotnet", "ElevenLabsProxy.dll"]
