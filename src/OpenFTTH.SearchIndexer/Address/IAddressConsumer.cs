using System;

namespace OpenFTTH.SearchIndexer.Address
{
    public interface IAddressConsumer : IDisposable
    {
        void Subscribe();
    }
}
