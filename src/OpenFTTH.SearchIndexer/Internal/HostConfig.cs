using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Typesense;
using Typesense.Setup;
using OpenFTTH.SearchIndexer.Address;

namespace OpenFTTH.SearchIndexer.Internal
{
    public static class HostConfig
    {
        public static IHost Configure()
        {
            var hostBuilder = new HostBuilder();
            ConfigureServices(hostBuilder);

            return hostBuilder.Build();
        }

        private static void ConfigureServices(IHostBuilder hostBuilder)
        {
            var node = new Node();
            node.Host = "localhost";
            node.Port = "8108";
            node.Protocol = "http";
            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<Startup>();
                services.AddTypesenseClient(options =>
                {
                    options.ApiKey = "Hu52dwsas2AdxdE";
                    options.Nodes = new List<Node>();
                    options.Nodes.Add(node);
                });
                services.AddScoped<IAddressConsumer,AddressConsumer>();
                services.AddScoped<ITypesenseClient,Client>();
            });
        }
    }
}
