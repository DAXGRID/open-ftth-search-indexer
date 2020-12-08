using System;
using System.Collections.Generic;
using System.Json;
using Topos.Config;
using OpenFTTH.SearchIndexer.Config;
using OpenFTTH.SearchIndexer.Serialization;
using Typesense;
using OpenFTTH.SearchIndexer.Model;
using Microsoft.Extensions.Logging;
using System.Linq;

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
            var retrieveCollections = _client.RetrieveCollection("Addresses");

            _logger.LogInformation("This is the request " + retrieveCollections.Result.ToString());
            Consume();
            /*
            var result =  _client.Search<Address>("Addresses",new SearchParameters{
                Text = "Horsens",
                QueryBy = "accessAddressDescription"
            });
            */

            //_logger.LogInformation(result.ToString());
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

        private void Consume()
        {
            var kafka = new KafkaSettings();
            kafka.DatafordelereTopic = "DAR";
            kafka.Server = "localhost:9092";
            kafka.PositionFilePath = "/tmp/";
            JsonValue c = "";
            JsonValue d = "";

            var AdresseList = new List<JsonValue>();
            var hussnumerList = new List<JsonValue>();

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

                          var typesenSeItems = MergeLists(AdresseList, hussnumerList);
                          AdresseList.Clear();
                          hussnumerList.Clear();
                          if (typesenSeItems.Count > 0)
                          {

                              foreach (var item in typesenSeItems)
                              {
                                  _logger.LogInformation(item.ToString());
                                  await _client.CreateDocument<Address>("Addresses", item);
                              }

                          }




                      }).Start();
        }

        public List<Address> MergeLists(List<JsonValue> addresseItems, List<JsonValue> hussnummerItems)
        {
            _logger.LogInformation("it got here");
            _logger.LogInformation("It is" + hussnummerItems.Count);
            var houseNumberLookup = hussnummerItems.Select(x => new KeyValuePair<string, JsonValue>(x["id_lokalId"], x)).ToDictionary(x => x.Key, x => x.Value);

            var newAddresseItems = new List<Address>();
            var newAdress = new Address();
            foreach (var adress in addresseItems)
            {
                var addressHouseNumber = (string)adress["houseNumber"];
                if (houseNumberLookup.TryGetValue(addressHouseNumber, out var house))
                {
                    var newAddres = new Address
                    {
                        id_lokalId = adress["id_lokalId"],
                        door = adress["door"],
                        doorPoint = adress["doorpoint"],
                        floor = adress["floor"],
                        unitAddressDescription = adress["unitAddressDescription"],
                        houseNumberId = house["id_lokalId"],
                        houseNumberDirection = house["houseNumberDirection"],
                        houseNumberText = house["houseNumberText"],
                        position = house["position"],
                        status = adress["status"],
                        accessAddressDescription = house["accessAddressDescription"]

                    };
                    _logger.LogInformation(newAddres.ToString());
                    newAddresseItems.Add(newAddres);

                }

            }
            _logger.LogInformation("THe number of items" + newAddresseItems.Count());
            return newAddresseItems;
        }

        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
        }
    }
}