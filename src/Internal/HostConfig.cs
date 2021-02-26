using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Typesense;
using Typesense.Setup;
using OpenFTTH.SearchIndexer.Consumer;
using Microsoft.Extensions.Configuration;
using System.IO;
using OpenFTTH.SearchIndexer.Config;
using Serilog;
using Serilog.Formatting.Compact;
using OpenFTTH.SearchIndexer.Database;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace OpenFTTH.SearchIndexer.Internal
{
    public static class HostConfig
    {
        public static IHost Configure()
        {
            var hostBuilder = new HostBuilder();
            ConfigureSerialization(hostBuilder);
            var builder = new ConfigurationBuilder();
            var configuration = SetupAppSettings(builder);

            ConfigureServices(hostBuilder, configuration);

            return hostBuilder.Build();
        }

        private static IConfigurationRoot SetupAppSettings(ConfigurationBuilder builder)
        {
            return builder
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
        }


        private static void ConfigureSerialization(IHostBuilder hostBuilder)
        {
            JsonConvert.DefaultSettings = (() =>
               {
                   var settings = new JsonSerializerSettings();
                   settings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                   settings.Converters.Add(new StringEnumConverter());
                   settings.TypeNameHandling = TypeNameHandling.Auto;
                   return settings;
               });
        }

        private static void ConfigureServices(IHostBuilder hostBuilder, IConfigurationRoot configuration)
        {
            var apiKey = "Hu52dwsas2AdxdE";
            var host = "localhost";
            var port = "8108";

            var node = new Node();
            node.Host = host;
            node.Port = port;
            node.Protocol = "http";
            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<Startup>();
                services.AddTypesenseClient(options =>
                {
                    options.ApiKey = apiKey;
                    options.Nodes = new List<Node>();
                    options.Nodes.Add(node);
                });
                services.AddScoped<IAddressConsumer, AddressConsumer>();
                services.AddScoped<IPostgresWriter, PSQLWriter>();
                services.Configure<KafkaSetting>(kafkaSettings =>
                                                configuration.GetSection("Kafka").Bind(kafkaSettings));

                services.Configure<DatabaseSetting>(databaseSettings =>
                                              configuration.GetSection("Database").Bind(databaseSettings));
                services.Configure<TypesenseSetting>(typesenseSettings =>
                                              configuration.GetSection("Typesense").Bind(typesenseSettings));                              

                services.AddLogging(configure =>
                {
                    var loggingConfiguration = new ConfigurationBuilder()
                  .AddEnvironmentVariables().Build();

                    var logger = new LoggerConfiguration()
                        .ReadFrom.Configuration(loggingConfiguration)
                        .Enrich.FromLogContext()
                        .WriteTo.Console(new CompactJsonFormatter())
                        .CreateLogger();

                    configure.AddSerilog(logger, true);
                });
            });
        }
    }
}
