using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using OpenFTTH.SearchIndexer.Internal;
using System;

namespace OpenFTTH.SearchIndexer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            using (var host = HostConfig.Configure())
            {
                Console.WriteLine("started");
                await host.StartAsync();
                await host.WaitForShutdownAsync();
            }
        }
    }
}
