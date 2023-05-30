using elasticSearchDemo.Models;
using Nest;
using System;
using System.Collections.Generic;
using System.Text;

namespace elasticSearchDemo
{
    /// <summary>
    /// 文档操作类
    /// </summary>
    public class DocumentOperate
    {
        private readonly ElasticClient _elasticClient;

        public DocumentOperate(ElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        /// <summary>
        /// 创建文档
        /// </summary>
        public void Create(string indexName, Person person)
        {
            var response = _elasticClient.Index(person, p => p.Id(person.Id).Index(indexName));
            var b = response.Id == person.Id.ToString();
        }

        /// <summary>
        /// 批量插入文档，有Id属性则使用自定的Id，否则es自动生成Id
        /// </summary>
        public void BulkInsert(string indexName, List<Person> persons)
        {
            BulkRequest bulkRequest = new BulkRequest(indexName);
            bulkRequest.Operations = new BulkOperationsCollection<IBulkOperation>();
            foreach (var item in persons)
            {
                var createOperation = new BulkCreateOperation<Person>(item);
                createOperation.Id = new Id(item.Id);
                bulkRequest.Operations.Add(createOperation);
            }
            var response = _elasticClient.Bulk(bulkRequest);
            if (response.ApiCall.Success)
            {
                Console.WriteLine("bulk success");
            }
        }

        /// <summary>
        /// 查询文档
        /// </summary>
        public void Get(string indexName, int docId)
        {
            var response = _elasticClient.Get<Person>(docId, p => p.Index(indexName));
            if (response.Source != null)
            {
                var b = response.Source.Id == docId;
            }
        }

        /// <summary>
        /// 修改文档
        /// </summary>
        public void Update(string indexName, Person person)
        {
            var response = _elasticClient.Index(person, p => p.Id(person.Id).Index(indexName));
            var b = response.Id == person.Id.ToString();
        }

        /// <summary>
        /// 删除文档
        /// </summary>
        public void Delete(string indexName, int docId)
        {
            var response = _elasticClient.Delete<Person>(docId, p => p.Index(indexName));
        }

        public void QueryMatch(string indexName)
        {
            var response = _elasticClient.Search<Person>(s => s
                    .Query(s => s.Match(m => m
                        .Field(f => f.Name).Query("zhangsan")))
                    .Index(indexName)
                );
        }

        public void MatchQueryField(string indexName)
        {
            var response = _elasticClient.Search<Person>(s => s
                    .Query(s => s.TermsSet(m => m
                        .Field(f => f.Name).Terms("zhangsan")))
                    .Index(indexName)
                    .Source(p => p.Includes(i => i.Field(f => f.Name).Field(f => f.Age)))
                );
        }

        public void AggsAvg(string indexName)
        {
            var response = _elasticClient.Search<Person>(s => s
                    .Index(indexName)
                    .Aggregations(a => a.
                        Average("avg_age", a => a.Field(f => f.Age))
                    )
                );
        }
    }
}
