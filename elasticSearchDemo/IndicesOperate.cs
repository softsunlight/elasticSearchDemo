using Nest;
using System;
using System.Collections.Generic;
using System.Text;

namespace elasticSearchDemo
{
    /// <summary>
    /// 索引操作类
    /// </summary>
    public class IndicesOperate
    {
        private readonly ElasticClient _elasticClient;

        public IndicesOperate(ElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }

        /// <summary>
        /// 创建索引
        /// </summary>
        public void CreateIndex(string indexName)
        {
            var response = _elasticClient.Indices.Create(indexName);
            if (response.Acknowledged)
            {
                var b = response.Index == indexName;
            }
        }

        /// <summary>
        /// 查询索引
        /// </summary>
        public void GetIndex(string indexName)
        {
            var response = _elasticClient.Indices.Get(indexName);
            var b = response.Indices.ContainsKey(indexName);
        }

        /// <summary>
        /// 查询所有索引
        /// </summary>
        public void GetIndex()
        {
            var response = _elasticClient.Cat.Indices();
            foreach (var item in response.Records)
            {
                Console.WriteLine(item.Index);
            }
        }

        /// <summary>
        /// 删除索引
        /// </summary>
        public void DeleteIndex(string indexName)
        {
            var response = _elasticClient.Indices.Delete(indexName);
            if (response.Acknowledged)
            {
                
            }
        }
    }
}
