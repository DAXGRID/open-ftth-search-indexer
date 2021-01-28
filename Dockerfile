FROM mcr.microsoft.com/dotnet/sdk:5.0-focal AS build-env
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY ./src/*.csproj ./
RUN dotnet restore


# Copy everything else and build
COPY . ./
RUN dotnet publish open-ftth-search-indexer.sln -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/aspnet:5.0
WORKDIR /app
COPY --from=build-env /app/out .


ENTRYPOINT ["dotnet", "open-ftth-search-indexer.dll"]

