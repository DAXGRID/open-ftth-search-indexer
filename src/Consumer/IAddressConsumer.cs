using System;
using System.Threading.Tasks;

namespace OpenFTTH.SearchIndexer.Consumer
{
    public interface IAddressConsumer : IDisposable
    {
        void Subscribe();
        void SubscribeBulk();
        void SubscribeRouteNetwork();
        bool IsBulkFinished();
        Task ProcessDataTypesense();
        void CreateTypesenseSchema();
        void CreateRouteSchema();
        Task UpdateNode();
        
    }
}
