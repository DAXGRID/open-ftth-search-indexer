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


            return hostBuilder.Build();
        }

        private static void ConfigureServices(IHostBuilder hostBuilder)
        {
            hostBuilder.ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<Startup>();
                services.AddTypesenseClient(options =>
                {
                    options.ApiKey = "";
                    options.Nodes = new List<Node>();
                });
                services.AddScoped<IAddressConsumer,AddressConsumer>();
            });
        }
    }
}
