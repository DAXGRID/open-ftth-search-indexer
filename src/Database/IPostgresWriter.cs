using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Npgsql;
using System.Json;

namespace OpenFTTH.SearchIndexer.Database
{
    public interface IPostgresWriter
    {
        void AddToPSQL(List<JsonObject> batch, string topic, string[] columns, string textConnection);

    }

}