# Stage 1: Build & Publish
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy csproj first (better layer caching)
COPY MatchQueueService/MatchQueueService.csproj MatchQueueService/
RUN dotnet restore MatchQueueService/MatchQueueService.csproj

# Copy everything else and publish
COPY . .
RUN dotnet publish MatchQueueService/MatchQueueService.csproj -c Release -o /app/publish

# Stage 2: Runtime
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

EXPOSE 8080

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "MatchQueueService.dll"]

