using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Documents.Client;

namespace CosmoDBDelete
{
    public class CosmoDBRepository
    {
        private const string CosmoDBURL = "https://localhost:8000/";
        private const string CosmoDBKey = "CosmoDBKey";
        private const string CosmoDBName = "test-db";
        private const string CosmoDBCollection = "Series";
        public async Task RemoveSeries()
        {
            try
            {
                Console.WriteLine("Run Only Once");

                //get the Series list to delete using the Microsoft.Azure.Cosmos package (It's probably easier to use MS Azure Documents.Client to search si)
                List<Series> list = new List<Series>();
                list.AddRange(await QueryItemAsync($"SELECT * From c WHERE c.SourceId in (583561,583562,583564,583565,583566,583567,583568,583569,583570,583571,583572,583573,583574,583575,583576,583577,583578,583579,583580,583581,583582,583583,583584,583586,583587,583588,583589,583590,583591,583592,583593,583594,583596,583597,583598,583599,583605,583606,583611,583614,583616,583626,583627,583628,583629,583630,583631,583658,583659,583660,583661,583662,583678,583679,583680,583681,583682,583683,583684,583685,583686,583687,583688,583689,583690)"));

                Console.WriteLine("Query returned empty set.");

                //For some reason we have to use the MS Azure Documents.Client package to remove records by _self link as it doesn't seem to work with the Microsoft.Azure.Cosmos package
                var client = new DocumentClient(new Uri(CosmoDBURL), CosmoDBKey);
                var options = new Microsoft.Azure.Documents.Client.RequestOptions { PartitionKey = Microsoft.Azure.Documents.PartitionKey.None };

                foreach (Series i in list)
                {
                    var doc = await client.ReadDocumentAsync(i._self, options);
                    if (doc != null)
                    {   //if we get a document from the _self link delete it
                        Console.WriteLine($"In Item {i.id} SourceId {i.SourceId}");
                        ResourceResponse<Microsoft.Azure.Documents.Document> document = await client.DeleteDocumentAsync(i._self, options);
                        Console.WriteLine($"Out Item {i.id} SourceId {i.SourceId}");
                    }
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message, ex.StackTrace);
            }
            
        }
        public async Task<List<Series>> QueryItemAsync(string query)
        {
            List<Series> result = new List<Series>();

            try
            {

                // Create a new instance of the Cosmos Client
                var cosmosClient = new CosmosClient
                    (CosmoDBURL,
                    CosmoDBKey,
                    new CosmosClientOptions()
                    {
                        ApplicationName = CosmoDBName,
                        ConnectionMode = Microsoft.Azure.Cosmos.ConnectionMode.Gateway
                    });

                var database = cosmosClient.GetDatabase(CosmoDBName);
                var container = database.GetContainer(CosmoDBCollection);

                QueryDefinition queryDefinition = new QueryDefinition(query);
                using (FeedIterator<Series> iterator = container.GetItemQueryIterator<Series>(queryDefinition: queryDefinition))
                {
                    while (iterator.HasMoreResults)
                    {
                        Microsoft.Azure.Cosmos.FeedResponse<Series> resultSet = await iterator.ReadNextAsync();
                        foreach (Series item in resultSet)
                        {
                            result.Add(item);
                        }
                    }
                }
            }
            catch (CosmosException e)
            {
                throw e;
            }
            catch (Exception e)
            {
                throw e;
            }

            return result;
        }
    }
}
