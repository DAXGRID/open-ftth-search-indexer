using System;
using Topos.Config;
using Topos.Serialization;

namespace  OpenFTTH.SearchIndexer.Serialization
{
    public static class ToposConfigSerializationExtension
    {
        public static void DatafordelerEventDeserializer(this StandardConfigurer<IMessageSerializer> configurer)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof(configurer));

            StandardConfigurer.Open(configurer)
                .Register(c => new DatafordelerEventDeserializer());
        }
    }
}
