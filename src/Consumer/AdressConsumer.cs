using System;
using System.Collections.Generic;
using System.Json;
using Topos.Config;
using OpenFTTH.SearchIndexer.Config;
using OpenFTTH.SearchIndexer.Serialization;
using Typesense;
using OpenFTTH.SearchIndexer.Model;
using Microsoft.Extensions.Logging;

namespace OpenFTTH.SearchIndexer.Consumer
{
    public class AddressConsumer : IAddressConsumer
    {
        private IDisposable _consumer;
        private List<IDisposable> _consumers = new List<IDisposable>();
        private ITypesenseClient _client;

        private ILogger<AddressConsumer> _logger;

        public AddressConsumer(ITypesenseClient client, ILogger<AddressConsumer> logger)
        {
            _client = client;
            _logger = logger;
        }

        public void Subscribe()
        {
            var adresseList = new List<Address>();
            var schema = new Schema
            {
                Name = "Addresses",
                Fields = new List<Field>
                {
                    new Field("id_lokalId", "string", false),
                    new Field("door", "string", false),
                    new Field("doorPoint", "string", false),
                    new Field("floor", "string", false),
                    new Field("unitAddressDescription", "string", false),
                    new Field("houseNumberId", "string", false),
                    new Field("houseNumberDirection", "string", false),
                    new Field("houseNumberText", "string", false),
                    new Field("position", "string", false),
                    new Field("accessAddressDescription", "string", false),
                    new Field("status","int32",false)
                },
                DefaultSortingField = "status"
            };
            var list = new List<JsonObject>();
            _client.CreateCollection(schema);
            var retrieveCollections = _client.RetrieveCollections();
            Consume();
        }

        public Address ConvertIntoAdress(JsonValue obj)
        {
            var address = new Address
            {
                id_lokalId = (string)obj["id_lokalId"],
                door = (string)obj["door"],
                doorPoint = (string)obj["doorPoint"],
                floor = (string)obj["floor"],
                unitAddressDescription = (string)obj["unitAddressDescription"],
                houseNumberId = (string)obj["houseNumber"],
                houseNumberDirection = (string)obj["houseNumberDirection"],
                houseNumberText = (string)obj["houseNumberText"],
                position = (string)obj["position"],
                accessAddressDescription = (string)obj["accessAddressDescription"],
                status = (int)obj["status"]

            };

            return address;

        }

        public void Consume()
        {
            var kafka = new KafkaSettings();
            kafka.DatafordelereTopic = "DAR";
            kafka.Server = "localhost:9092";
            kafka.PositionFilePath = "/tmp/";
            JsonValue c = "";
            JsonValue d = "";

            var AdresseList = new List<JsonValue>();
            var hussnumerList = new List<JsonValue>();
            var newItems = new List<JsonValue>();
            var typesenSeItems = new List<Address>();

            var consumer = _consumer = Configure
                      .Consumer(kafka.DatafordelereTopic, c => c.UseKafka(kafka.Server))
                      .Serialization(s => s.DatafordelerEventDeserializer())
                      .Topics(t => t.Subscribe(kafka.DatafordelereTopic))
                      .Positions(p => p.StoreInFileSystem(kafka.PositionFilePath))
                      .Handle(async (messages, context, token) =>
                      {
                          try
                          {
                              foreach (var message in messages)
                              {

                                  if (message.Body is JsonObject)
                                  {

                                      var obj = JsonObject.Parse(message.Body.ToString());
                                      if (obj["type"] == "AdresseList")
                                      {

                                          AdresseList.Add(obj);

                                      }
                                      else if (obj["type"] == "HusnummerList")
                                      {
                                          hussnumerList.Add(obj);


                                      }

                                  }

                              }
                          }
                          finally
                          {
                              newItems = MergeLists(AdresseList, hussnumerList);
                              foreach (var item in newItems)
                              {
                                  typesenSeItems.Add(ConvertIntoAdress(item));
                              }
                              _logger.LogInformation("This is the number of items " + typesenSeItems.Count);
                              await _client.ImportDocuments<Address>("Adresses", typesenSeItems, 1000, ImportType.Create);

                          }

                      }).Start();



        }

        public List<JsonValue> MergeLists(List<JsonValue> addresseItems, List<JsonValue> hussnummerItems)
        {
            var newAddresseItems = new List<JsonValue>();
            foreach (var adress in addresseItems)
            {
                foreach (var house in hussnummerItems)
                {
                    if (adress["houseNumber"].Equals(house["id_lokalId"]))
                    {
                        adress["houseNumberDirection"] = house["houseNumberDirection"];
                        adress["houseNumberText"] = house["houseNumberText"];
                        adress["position"] = house["position"];
                        adress["accessAddressDescription"] = house["accessAddressDescription"];

                        newAddresseItems.Add(adress);
                    }
                }
            }
            return newAddresseItems;
        }


        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
        }
    }
}