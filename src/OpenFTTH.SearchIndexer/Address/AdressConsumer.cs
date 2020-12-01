using System;
using System.Collections.Generic;
using System.Json;
using System.Text.Json;
using Topos.Config;
using OpenFTTH.SearchIndexer.Config;
using OpenFTTH.SearchIndexer.Serialization;
using Typesense;
using Microsoft.Extensions.DependencyInjection;
using Typesense.Setup;

namespace OpenFTTH.SearchIndexer.Address
{
    public class AddressConsumer : IAddressConsumer
    {
        private IDisposable _consumer;
        private List<IDisposable> _consumers = new List<IDisposable>();



        public void Subscribe()
        {

            var provider = new ServiceCollection()
             .AddTypesenseClient(config =>
       {
           config.ApiKey = "Hu52dwsas2AdxdE";
           config.Nodes = new List<Node>
      {
            new Node
            {
                Host = "localhost",
                Port = "8108", Protocol = "http"
            }
      };
       }).BuildServiceProvider();
            var typesenseClient = provider.GetService<ITypesenseClient>();
            var adresseList = new List<Address>();


            var schema = new Schema
            {
                Name = "Addresses",
                Fields = new List<Field>
                {
                    new Field("id_lokalId", "string", false),
                    new Field("houseNumber", "string", false),
                    new Field("status","int32",false),
                    new Field("accessAddress", "string", false),
                },
                DefaultSortingField = "status"
            };

            var schema1 = new Schema
            {
                Name = "AdressesImport",
                Fields = new List<Field>
                {
                    new Field("id_lokalId", "string", false),
                    new Field("houseNumber", "string", false),
                    new Field("status","int32",false),
                    new Field("accessAddress", "string", false),
                },
                DefaultSortingField = "status"
            };

            var list = new List<JsonObject>();

            var kafka = new KafkaSetting();
            typesenseClient.CreateCollection(schema);
            typesenseClient.CreateCollection(schema1);
            var retrieveCollections = typesenseClient.RetrieveCollections();
            //Console.WriteLine($"Retrieve collections: {JsonSerializer.Serialize(retrieveCollections)}");


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
                          foreach (var message in messages)
                          {
                              if (message.Body is JsonObject)
                              {

                                  var obj = JsonObject.Parse(message.Body.ToString());
                                  if (obj["type"] == "HusnummerList")
                                  {
                                      var newobj = new
                                      {
                                          id_lokalId = obj["id_lokalId"],
                                          houseNumber = obj["houseNumberText"],
                                          status = (int)obj["status"],
                                          accessAdress = obj["accessAddressDescription"]
                                      };

                                      //Console.WriteLine($"Retrieve collections: {JsonSerializer.Serialize(retrieveCollections)}");
                                      //var newObj = new Address(obj["id_lokalId"], obj["houseNumberText"], (int)obj["status"], obj["accessAddressDescription"]);
                                      var addressOne = new Address
                                      {
                                          id_lokalId = (string)obj["id_lokalId"],
                                          houseNumber = (string)obj["houseNumberText"],
                                          status = (int)obj["status"],
                                          accessAddress = (string)obj["accessAddressDescription"]
                                      };

                                      await typesenseClient.CreateDocument<Address>("Addresses", addressOne);

                                      adresseList.Add(addressOne);

                                      if (adresseList.Count > 40)
                                      {
                                        await typesenseClient.ImportDocuments<Address>("AdressesImport", adresseList, 40, ImportType.Create);
                                        adresseList.Clear();
                                      }
                                  }

                              }
                          }
                      }).Start();

            Console.WriteLine($"Retrieve collections: {JsonSerializer.Serialize(retrieveCollections)}");
            var doc = typesenseClient.RetrieveDocument<Address>("Addresses", "1");
            var search = new SearchParameters
            {
                Text = "Smed",
                QueryBy = "accessAddress"
            };
            var q = typesenseClient.Search<Address>("Addresses", search);

            //Console.WriteLine(JsonSerializer.Serialize(q));




        }

        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
        }
    }
}