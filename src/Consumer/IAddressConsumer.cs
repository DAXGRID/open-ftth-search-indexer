using System;

namespace OpenFTTH.SearchIndexer.Consumer
{
    public interface IAddressConsumer : IDisposable
    {
        void Subscribe();
        void SubscribeBulk();
        bool IsBulkFinished();
        void ProcessDataTypesense();
        void CreateTypesenseSchema();
        void searchTypesense();
    }
}
