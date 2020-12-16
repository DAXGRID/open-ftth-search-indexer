using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Npgsql;
using System.Json;
using OpenFTTH.SearchIndexer.Model;

namespace OpenFTTH.SearchIndexer.Database
{
    public interface IPostgresWriter
    {
        void AddToPSQL(List<JsonObject> batch, string topic, string[] columns, string textConnection);
         List<Address> JoinTables(string adresseColummn, string houseColumn, string textConnection);

    }

}