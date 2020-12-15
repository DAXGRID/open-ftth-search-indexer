using OpenFTTH.SearchIndexer.Config;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Linq;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;
using System.Json;

namespace OpenFTTH.SearchIndexer.Database
{
    public class PSQLWriter : IPostgresWriter
    {
        private readonly ILogger<PSQLWriter> _logger;
        private readonly DatabaseSetting _databaseSetting;
        private readonly KafkaSetting _kafkaSetting;
        private bool postgisExecuted;

        public PSQLWriter(
           ILogger<PSQLWriter> logger,
           IOptions<DatabaseSetting> databaseSetting,
           IOptions<KafkaSetting> kafkaSetting
           )
        {
            _logger = logger;
            _databaseSetting = databaseSetting.Value;
            _kafkaSetting = kafkaSetting.Value;
            NpgsqlConnection.GlobalTypeMapper.UseNetTopologySuite();
        }

        public void AddToPSQL(List<JsonObject> batch, string topic, string[] columns, string textConnection)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(textConnection))
            {
                connection.Open();
                createTemporaryTable(topic + "_temp", columns, connection);
                createTable(topic, columns, connection);

                UpsertData(batch, topic + "_temp", columns, connection);
                InsertOnConflict(topic + "_temp", topic, columns, connection);
            }

            _logger.LogInformation("Wrote in the database");
        }

        private void createTemporaryTable(string topic, string[] columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();

            foreach (var column in columns)
            {
                if (column == "position")
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }
            }

            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);
            var tableColumnsText = tableColumns.ToString();

            var tableCommandText = @$"Create temporary table  {topic}  (  {tableColumns} );";


            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {

                command.ExecuteNonQuery();
            }

            _logger.LogInformation("Temporary Table " + topic + " created");
        }

        public void JoinTables(string adresseColummn, string houseColumn, NpgsqlConnection connection)
        {
            var tableCommandText = @$"SELECT * FROM adresselist INNER JOIN husnummerlist ON (adresselist.{adresseColummn} = husnummerlist.{houseColumn});";
            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation("Tables were joined");
            
        }

        private void InsertOnConflict(string tempTable, string table, string[] columns, NpgsqlConnection conn)
        {
            string id;
            var tempColumns = new StringBuilder();
            var onConflictColumns = new StringBuilder();

            if (columns.Contains("geo"))
            {
                id = "gml_id";
            }
            else
            {
                id = "id_lokalId";
            }

            foreach (var column in columns)
            {
                tempColumns.Append(tempTable + "." + column + ",");
                onConflictColumns.Append(column + " = " + "EXCLUDED." + column + ",");
            }

            tempColumns = tempColumns.Remove(tempColumns.Length - 1, 1);
            onConflictColumns = onConflictColumns.Remove(onConflictColumns.Length - 1, 1);

            var commandText = @$" INSERT INTO  {table}  SELECT DISTINCT ON (1)  {tempColumns} FROM   {tempTable}  ON CONFLICT ( {id}  ) DO UPDATE  SET  {onConflictColumns}  ;";

            using (var command = new NpgsqlCommand(commandText, conn))
            {
                command.ExecuteNonQuery();
            }
        }
        private void UpsertData(List<JsonObject> batch, string topic, string[] columns, NpgsqlConnection conn)
        {
            var tableColumns = new StringBuilder();
            var geometryFactory = new GeometryFactory();
            var rdr = new WKTReader(geometryFactory);

            foreach (var column in columns)
            {

                tableColumns.Append(column + ",");

            }
            tableColumns = tableColumns.Remove(tableColumns.Length - 1, 1);

            var comand = @$"COPY  {topic}   (  {tableColumns} ) FROM STDIN (FORMAT BINARY)";

            using (var writer = conn.BeginBinaryImport(comand))
            {
                foreach (var document in batch)
                {
                    writer.StartRow();
                    foreach (var column in columns)
                    {
                        if (column == "position" )
                        {
                            // TODO add environment variable
                            rdr.DefaultSRID = _databaseSetting.GeoSRID;
                            var c = rdr.Read((string)document[column]);
                            writer.Write(c);
                        }
                        else
                        {
                            writer.Write((string)document[column]);
                        }
                    }
                }

                writer.Complete();
                batch.Clear();
            }
        }

        private void createTable(string topic, string[] columns, NpgsqlConnection connection)
        {
            var tableColumns = new StringBuilder();
            string id;

            if (columns.Contains("geo"))
            {
                id = "gml_id";
            }
            else
            {
                id = "id_lokalId";
            }

            foreach (var column in columns)
            {

                if (column == "position" )
                {
                    tableColumns.Append(column + " geometry" + ",");
                }
                else
                {
                    tableColumns.Append(column + " varchar" + ",");
                }

            }

            var tableCommandText = @$"Create table IF NOT EXISTS   {topic}   (  {tableColumns}  PRIMARY KEY   (  {id} ) );";
            _logger.LogInformation(tableCommandText);

            using (NpgsqlCommand command = new NpgsqlCommand(tableCommandText, connection))
            {
                command.ExecuteNonQuery();
            }

            _logger.LogInformation("Table " + topic + " created");
        }

    }
}
