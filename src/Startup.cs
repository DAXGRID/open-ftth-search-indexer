using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using OpenFTTH.SearchIndexer.Consumer;
using System;

namespace OpenFTTH.SearchIndexer
{
    public class Startup : IHostedService
    {
        private readonly IHostApplicationLifetime _applicationLifetime;
        private IAddressConsumer _consumer;
        private bool _isStopping;

        public Startup(IHostApplicationLifetime applicationLifetime, IAddressConsumer consumer)
        {
            _applicationLifetime = applicationLifetime;
            _consumer = consumer;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _applicationLifetime.ApplicationStarted.Register(OnStarted);
            _applicationLifetime.ApplicationStopping.Register(OnStopped);

            MarkAsReady();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        private void MarkAsReady()
        {
            File.Create("/tmp/healthy");
        }

        private void OnStarted()
        {
            // _consumer.CreateTypesenseSchema();
             _consumer.CreateRouteSchema();
             _consumer.SubscribeRouteNetwork();

            //TODO use enviroment variable

            // var bulkSetup = true;
            // if (bulkSetup)
            // {
            //     _consumer.SubscribeBulk();
            //     while (!_consumer.IsBulkFinished() && !_isStopping)
            //     {
            //         Thread.Sleep(1000);
            //     }
            //     // _consumer.ProcessDataTypesense();
            //     _consumer.Dispose();
            // }
            _consumer.Dispose();
            // //_consumer.Subscribe();

        }

        private void OnStopped()
        {
            _isStopping = true;
            _consumer.Dispose();
            // Dispose
        }
    }
}
