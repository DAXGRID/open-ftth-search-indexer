namespace OpenFTTH.SearchIndexer.Model
{
    public class Address
    {
        public string id_lokalId { get; set; }
        public string door { get; set; }
        public string doorPoint { get; set; }
        public string floor { get; set; }
        public string unitAddressDescription { get; set; }
        public string houseNumberId { get; set; }
        public int status { get; set; }
        public string houseNumberDirection { get; set; }
        public string houseNumberText { get; set; }
        public string position { get; set; }
        public string accessAddressDescription { get; set; }

        public string roadName {get;set;}
        

    }
}