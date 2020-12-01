using System;

namespace OpenFTTH.SearchIndexer.Consumer
{
    public interface IAddressConsumer : IDisposable
    {
        void Subscribe();
    }
}
