using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using ChannelManager.DynamoDB.Interfaces;
using System.Collections.Generic;

namespace ChannelManager.DynamoDB.Services
{
    public class DynamoDbService : IDynamoDbService
    {
        public readonly DynamoDBContext DbContext;
        public AmazonDynamoDBClient DynamoClient;
        public DynamoDbService(string AWSAccessKey, string AWSSecretKey, RegionEndpoint AWSRegionEndpoint)
        {
            var credentials = new BasicAWSCredentials(AWSAccessKey, AWSSecretKey);
            DynamoClient = new AmazonDynamoDBClient(credentials, AWSRegionEndpoint);
            DbContext = new DynamoDBContext(DynamoClient, new DynamoDBContextConfig
            {
                //Definir a propriedade Consistent como true garante que você sempre obterá as últimas 
                ConsistentRead = true,
                SkipVersionCheck = true
            });
        }

        /// <summary>
        /// The CreateTable method create a table of generic object received by parameter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"> Object represents the table for create</param>
        /// <param name="primaryKey">Primary Key/Partition Key for table</param>
        /// <param name="rangeKey">Range Key for table</param>
        public void CreateTable<T>(T table, string primaryKey, string rangeKey)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = typeof(T).Name,
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                },
                KeySchema = new List<KeySchemaElement>
                    {
                        new KeySchemaElement
                        {
                            AttributeName = primaryKey,
                            KeyType = "HASH"
                        },
                        new KeySchemaElement
                        {
                            AttributeName = rangeKey,
                            KeyType = "RANGE"
                        },
                    },
                AttributeDefinitions = new List<AttributeDefinition>()
                    {
                        new AttributeDefinition()
                        {
                            AttributeName = primaryKey,AttributeType = "S"
                        },
                        new AttributeDefinition()
                        {
                            AttributeName = rangeKey,AttributeType = "N"
                        }
                    }
            };
            CreateTableResponse createTableResponse = DynamoClient.CreateTable(createTableRequest);

            while (DynamoClient.DescribeTable(createTableRequest.TableName).Table.TableStatus != "ACTIVE")
            {
                System.Threading.Thread.Sleep(500);
            }
        }


        /// <summary>
        /// The Store method allows you to save an object to DynamoDb
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        public void Store<T>(T item) where T : new()
        {
            DbContext.Save(item);
        }

        /// <summary>
        /// The BatchStore Method allows you to store a list of items of type T to DynamoDb
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items"></param>
        public void BatchStore<T>(IEnumerable<T> items) where T : class
        {
            var itemBatch = DbContext.CreateBatchWrite<T>();
            foreach (var item in items)
                itemBatch.AddPutItem(item);

            itemBatch.Execute();
        }
        /// <summary>
        /// Use GetAll method to retrieve all items in a table
        /// <remarks>[CAUTION] This operation can be very expensive if your table is large</remarks>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetAll<T>() where T : class
        {
            IEnumerable<T> items = DbContext.Scan<T>();
            return items;
        }

        /// <summary>
        /// Retrieves an item based on a string search key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetItem<T>(string key) where T : class
        {
            return DbContext.Load<T>(key);
        }
        /// <summary>
        /// Retrieves an item based on a int search key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public T GetItem<T>(int key) where T : class
        {
            return DbContext.Load<T>(key);
        }

        /// <summary>
        /// Retrieves an IEnumerable of generic object based on array of ScanCondition
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="scanCondition"></param>
        /// <returns></returns>
        public IEnumerable<T> Scan<T>(ScanCondition[] scanCondition) where T : class
        {
            return DbContext.Scan<T>(scanCondition);
        }

        /// <summary>
        /// Method Updates and existing item in the table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        public void UpdateItem<T>(T item) where T : class
        {
            T savedItem = DbContext.Load(item);

            if (savedItem == null)
            {
                throw new AmazonDynamoDBException("The item does not exist in the Table");
            }

            DbContext.Save(item);
        }

        /// <summary>
        /// Deletes an Item from the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        public void DeleteItem<T>(T item)
        {
            var savedItem = DbContext.Load(item);
            if (savedItem == null)
                throw new AmazonDynamoDBException("The item does not exist in the Table");

            DbContext.Delete(item);
        }

    }
}
