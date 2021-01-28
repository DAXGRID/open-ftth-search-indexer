using System;
using System.Collections.Generic;
using System.Json;
using Topos.Config;
using Topos.InMem;
using OpenFTTH.SearchIndexer.Config;
using OpenFTTH.SearchIndexer.Serialization;
using Typesense;
using Microsoft.Extensions.Logging;
using OpenFTTH.SearchIndexer.Database;
using System.Timers;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using OpenFTTH.Events.RouteNetwork;
using DAX.EventProcessing.Dispatcher;
using DAX.EventProcessing.Dispatcher.Topos;
using Serilog;
using OpenFTTH.SearchIndexer.Model;

namespace OpenFTTH.SearchIndexer.Consumer
{
    public class AddressConsumer : IAddressConsumer
    {
        private IDisposable _consumer;
        private List<IDisposable> _consumers = new List<IDisposable>();
        private ITypesenseClient _client;

        private readonly KafkaSetting _kafkaSetting;
        private readonly DatabaseSetting _databaseSetting;

        private ILogger<AddressConsumer> _logger;
        private readonly IPostgresWriter _postgresWriter;

        private Dictionary<string, List<JsonObject>> _topicList = new Dictionary<string, List<JsonObject>>();
        private bool _isBulkFinished;
        private DateTime _lastMessageReceivedBulk;
        private Timer _bulkInsertTimer;

        private IToposTypedEventObservable<RouteNetworkEditOperationOccuredEvent> _eventDispatcher;

        public AddressConsumer(ITypesenseClient client, ILogger<AddressConsumer> logger, IPostgresWriter postgresWriter, IOptions<KafkaSetting> kafkaSetting, IOptions<DatabaseSetting> databaseSetting)
        {
            _client = client;
            _logger = logger;
            _postgresWriter = postgresWriter;
            _kafkaSetting = kafkaSetting.Value;
            _databaseSetting = databaseSetting.Value;
        }

        public void Subscribe()
        {
            Consume();
        }

        public void CreateTypesenseSchema()
        {
            var schema = new Schema
            {
                Name = "Addresses",
                Fields = new List<Field>
                {
                    new Field("id_lokalId", "string", false),
                    new Field("door", "string", false),
                    new Field("floor", "string", false),
                    new Field("unitAddressDescription", "string", false),
                    new Field("houseNumberId", "string", false),
                    new Field("houseNumberDirection", "string", false),
                    new Field("houseNumberText", "string", false),
                    new Field("position", "string", false),
                    new Field("accessAddressDescription", "string", false),
                    new Field("roadName","string",false),
                    new Field("status","int32",false)
                },
                DefaultSortingField = "status"
            };

            _client.CreateCollection(schema);
        }

        public void CreateRouteSchema()
        {
            var schema = new Schema
            {
                Name = "RouteNodes",
                Fields = new List<Field>
                {
                    new Field("id","string",false),
                    new Field("incrementalId","int32",false),
                    new Field("name","string",false),
                    new Field("coordinates","string",false)
                },
                DefaultSortingField = "incrementalId"
            };

            _client.CreateCollection(schema);
        }

        private void CheckBulkStatus(object source, ElapsedEventArgs e)
        {
            var elapsedTime = DateTime.UtcNow - _lastMessageReceivedBulk;

            _logger.LogInformation($"Elapsed time: {elapsedTime.TotalSeconds}");

            if (elapsedTime.TotalSeconds > 10)
            {
                _isBulkFinished = true;
            }
        }

        public void SubscribeBulk()
        {
            _logger.LogInformation("This is the topic" + _kafkaSetting.DatafordelereTopic);

            var AdresseList = new List<JsonValue>();
            var hussnumerList = new List<JsonValue>();

            _bulkInsertTimer = new Timer();
            _bulkInsertTimer.Elapsed += new ElapsedEventHandler(CheckBulkStatus);
            _bulkInsertTimer.Interval = 1000;

            _bulkInsertTimer.Start();
            var consumer = _consumer = Configure
                      .Consumer(_kafkaSetting.DatafordelereTopic, c => c.UseKafka(_kafkaSetting.Server))
                      .Serialization(s => s.DatafordelerEventDeserializer())
                      .Topics(t => t.Subscribe(_kafkaSetting.DatafordelereTopic))
                      .Positions(p => p.StoreInFileSystem(_kafkaSetting.PositionFilePath))
                      .Handle(async (messages, context, token) =>
                      {
                          foreach (var message in messages)
                          {
                              _lastMessageReceivedBulk = DateTime.UtcNow;
                              if (message.Body is JsonObject)
                              {
                                  if (!_topicList.ContainsKey(_kafkaSetting.DatafordelereTopic))
                                  {
                                      _topicList.Add(_kafkaSetting.DatafordelereTopic, new List<JsonObject>());
                                      _topicList[_kafkaSetting.DatafordelereTopic].Add((JsonObject)message.Body);
                                  }
                                  else
                                  {

                                      _topicList[_kafkaSetting.DatafordelereTopic].Add((JsonObject)message.Body);
                                  }

                                  if (_topicList[_kafkaSetting.DatafordelereTopic].Count >= 10000)
                                  {

                                      foreach (var obj in _databaseSetting.Values)
                                      {

                                          var tableName = obj.Key;

                                          var columns = obj.Value.Split(",");

                                          var batch = CheckObjectType(_topicList[_kafkaSetting.DatafordelereTopic], tableName);
                                          if (batch != null)
                                          {
                                              _postgresWriter.AddToPSQL(batch, tableName, columns, _databaseSetting.ConnectionString);
                                          }
                                      }
                                      _topicList[_kafkaSetting.DatafordelereTopic].Clear();
                                  }
                              }
                          }
                      }).Start();
        }

        public void SubscribeRouteNetwork()
        {
            int incremantalId = 0;

            var serilogLogger = new LoggerConfiguration()
                            .MinimumLevel.Debug()
                            .Enrich.FromLogContext()
                            .WriteTo.Console()
                            .CreateLogger();

            var loggerFactory = (ILoggerFactory)new LoggerFactory();
            loggerFactory.AddSerilog(serilogLogger);

            _eventDispatcher = new ToposTypedEventObservable<RouteNetworkEditOperationOccuredEvent>(loggerFactory.CreateLogger<ToposTypedEventMediator<RouteNetworkEditOperationOccuredEvent>>());

            var kafkaConsumer = _eventDispatcher.Config("route_network_event_" + Guid.NewGuid(), c => c.UseKafka("")
                             .WithCertificate("/home/mihai/Certificates/kafka-aura-prod.crt"))
                             .Positions(p => p.StoreInMemory(new InMemPositionsStorage()))
                             .Topics(t => t.Subscribe("domain.route-network"))
                             .Handle(async (messages, context, token) =>
                             {
                                 foreach (var message in messages)
                                 {
                                     if (message.Body is RouteNetworkEditOperationOccuredEvent)
                                     {
                                         var route = (RouteNetworkEditOperationOccuredEvent)message.Body;
                                         if (route.RouteNetworkCommands != null)
                                         {
                                             foreach (var command in route.RouteNetworkCommands)
                                             {
                                                 if (command.RouteNetworkEvents != null)
                                                 {
                                                     foreach (var routeNetworkEvent in command.RouteNetworkEvents)
                                                     {
                                                         switch (routeNetworkEvent)
                                                         {
                                                             case RouteNodeAdded domainEvent:
                                                                 incremantalId++;
                                                                 if (domainEvent.NamingInfo.Name != null)
                                                                 {
                                                                     var node = new RouteNode
                                                                     {
                                                                         id = domainEvent.NodeId,
                                                                         incrementalId = incremantalId,
                                                                         name = domainEvent.NamingInfo.Name,
                                                                         coordinates = domainEvent.Geometry
                                                                     };
                                                                     await addRouteNode(node);
                                                                 }
                                                                 break;

                                                             case RouteNodeMarkedForDeletion domainEvent:
                                                                 await DeleteRouteNode(domainEvent.NodeId);
                                                                 break;
                                                         }
                                                     }
                                                 }
                                             }
                                         }
                                     }
                                 }
                             }).Start(); ;


        }


        public bool IsBulkFinished()
        {
            return _isBulkFinished;
        }

        private async Task addRouteNode(RouteNode node)
        {
            await _client.CreateDocument<RouteNode>("RouteNodes", node);
        }

        private async Task DeleteRouteNode(Guid id)
        {
            await _client.DeleteDocument<RouteNode>("RouteNodes", id.ToString());
        }

        public async Task ProcessDataTypesense()
        {
            var typsenseItems = _postgresWriter.JoinTables("housenumber", "id_lokalid", _databaseSetting.ConnectionString);
            _logger.LogInformation("Number of objects " + typsenseItems.Count.ToString());
            await _client.ImportDocuments("Addresses", typsenseItems, typsenseItems.Count, ImportType.Create);
        }

        private List<JsonObject> CheckObjectType(List<JsonObject> items, string tableName)
        {
            var batch = new List<JsonObject>();

            foreach (var item in items)
            {
                if (item["type"] == tableName)
                {
                    batch.Add(item);
                }
            }

            return batch;
        }

        private void Consume()
        {
            var kafka = new KafkaSetting();
            kafka.DatafordelereTopic = "DAR";
            kafka.Server = "localhost:9092";
            kafka.PositionFilePath = "/tmp/";

            var AdresseList = new List<JsonValue>();
            var hussnumerList = new List<JsonValue>();

            DateTime waitStartTimestamp = DateTime.UtcNow;

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
                                  var messageTime = DateTime.UtcNow;

                                  TimeSpan timespan = waitStartTimestamp - messageTime;

                                  if (timespan.TotalSeconds > 10)
                                  {
                                      _logger.LogInformation("It worked");
                                  }
                              }
                          }
                      }).Start();

        }


        public void Dispose()
        {
            _consumers.ForEach(x => x.Dispose());
            if (!(_bulkInsertTimer is null))
            {
                _bulkInsertTimer.Stop();
                _bulkInsertTimer.Dispose();
            }
        }
    }
}
