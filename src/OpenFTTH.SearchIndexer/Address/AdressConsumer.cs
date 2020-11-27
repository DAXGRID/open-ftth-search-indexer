using System;
using System.Collections.Generic;
using System.Json;
using Topos.Config;
using OpenFTTH.SearchIndexer.Config;

namespace OpenFTTH.SearchIndexer.Address
{
    public class AddressConsumer : IAddressConsumer
    {
         private IDisposable _consumer;
         private List<IDisposable> _consumers = new List<IDisposable>();


        public void Subscribe()
        {
            var list = new List<JsonObject>();
            var kafka = new KafkaSetting();
            kafka.DatafordelereTopic = "DAR";
        }

        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
        }
    }
}