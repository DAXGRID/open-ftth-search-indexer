using System;
using System.Collections.Generic;
using System.Json;
using Topos.Config;
using OpenFTTH.SearchIndexer.Config;
using OpenFTTH.SearchIndexer.Serialization;

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
            kafka.Server = "localhost:9092";
            kafka.PositionFilePath = "/tmp/";

             var consumer = _consumer = Configure
                       .Consumer(kafka.DatafordelereTopic, c => c.UseKafka(kafka.Server))
                       .Serialization(s => s.DatafordelerEventDeserializer())
                       .Topics(t => t.Subscribe(kafka.DatafordelereTopic))
                       .Positions(p => p.StoreInFileSystem(kafka.PositionFilePath))
                       .Handle(async (messages, context, token) =>
                       {
                           foreach(var message in messages)
                           {
                               Console.WriteLine("here");
                               if(message.Body is JsonObject)
                               {
                                   Console.WriteLine(message.Body.ToString());
                               }
                           }
                       }).Start();


        }

        public void Dispose()
        {
           // _consumers.ForEach(x => x.Dispose());
        }
    }
}