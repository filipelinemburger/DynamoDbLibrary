using Amazon.DynamoDBv2.DataModel;
using System.Collections.Generic;

namespace ChannelManager.DynamoDB.Interfaces
{
    public interface IDynamoDbService
    {
        void CreateTable<T>(T table, string primaryKey, string rangeKey);
        void Store<T>(T item) where T : new();
        void BatchStore<T>(IEnumerable<T> items) where T : class;
        IEnumerable<T> GetAll<T>() where T : class;
        T GetItem<T>(string key) where T : class;
        T GetItem<T>(int key) where T : class;
        IEnumerable<T> Scan<T>(ScanCondition[] scanCondition) where T : class;
        void UpdateItem<T>(T item) where T : class;
        void DeleteItem<T>(T item);
    }
}
