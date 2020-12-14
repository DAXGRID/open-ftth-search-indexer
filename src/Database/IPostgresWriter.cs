using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Npgsql;

namespace OpenFTTH.SearchIndexer.Database
{
    public interface IPostgresWriter
    {
        void AddToPSQL(List<JObject> batch, string topic, string[] columns);

    }

}