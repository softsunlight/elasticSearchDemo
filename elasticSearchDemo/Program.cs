using Dapper;
using Elasticsearch.Net;
using MySql.Data.MySqlClient;
using Nest;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace elasticSearchDemo
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //var pool = new SingleNodeConnectionPool(new Uri("https://192.168.220.128:9200"));

            //var settings = new ConnectionSettings(pool)
            //    .CertificateFingerprint("2f0c21fdc5d9d05b423152b3d485abcffe9093deb9c93d67f3b82cef913a1dbb")
            //    .BasicAuthentication("elastic", "ko0Hp1Wxhx1ZTJTtyFOI");

            var pool = new SingleNodeConnectionPool(new Uri("https://127.0.0.1:9200"));

            var settings = new ConnectionSettings(pool)
                .CertificateFingerprint("F4:BC:31:1C:EB:54:20:2C:1D:F8:81:90:44:BE:A6:63:47:CE:9D:7D:80:30:A1:EC:7B:87:4A:E4:00:4D:66:D7")
                .BasicAuthentication("elastic", "e1dkAdhm3l7ZD+qTsBhE");

            var client = new ElasticClient(settings);

            #region 商户任务表查询
            var param = new MerchantTaskParam()
            {
                BeginTime = new DateTime(2023, 01, 31, 19, 54, 0),
                EndTime = new DateTime(2023, 01, 31, 20, 34, 0),
                MerchantNo = "A2022102161137",
            };
            List<Func<QueryContainerDescriptor<MerchantTask>, QueryContainer>> queryList = new List<Func<QueryContainerDescriptor<MerchantTask>, QueryContainer>>();
            if (!string.IsNullOrWhiteSpace(param.SerialNo))
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.SerialNo).Query(param.SerialNo)));
            }
            if (!string.IsNullOrWhiteSpace(param.MerchantNo))
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.MerchantNo).Query(param.MerchantNo)));
            }
            if (!string.IsNullOrWhiteSpace(param.AccountId))
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.AccountId).Query(param.AccountId)));
            }
            if (!string.IsNullOrWhiteSpace(param.DeviceNo))
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.DeviceNo).Query(param.DeviceNo)));
            }
            if (!string.IsNullOrWhiteSpace(param.ShortMessage))
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.ShortMessage).Query(param.ShortMessage)));
            }
            if (param.TaskResult != null)
            {
                queryList.Add(p => p.Match(p => p.Field(p => p.TaskResult).Query(param.TaskResult.ToString())));
            }
            if (param.BeginTime != null)
            {
                queryList.Add(p => p.DateRange(p => p.Field(p=>p.CreateTime).GreaterThanOrEquals(param.BeginTime.Value)));
            }
            if (param.EndTime != null)
            {
                queryList.Add(p => p.DateRange(p => p.Field(p => p.CreateTime).LessThanOrEquals(param.EndTime.Value)));
            }
            //20230131195500295437710016124
            var response = client.Search<MerchantTask>(s =>
                s.Query(q =>
                    q.Bool(m =>
                        m.Must(queryList)
                    )
                )
                .From(0)
                .Size(10)
                .Index("merchant_task"));
            Console.ReadLine();
            #endregion

            #region 同步task_details表数据
            //IDbConnection connection = new MySqlConnection("");
            //var total = connection.ExecuteScalar<int>("SELECT COUNT(0) FROM task_center.task_details;");
            //int pageNo = 1;
            //int pageSize = 200;
            //do
            //{
            //    var list = connection.Query<TaskDetails>($"SELECT task_no as TaskNo,serial_no as SerialNo,merchant_no as MerchantNo,outer_serial_no as OuterSerialNo,account_id as AccountId,device_no as DeviceNo,js_url as JsUrl,param_map as ParamMap,resource_type as ResourceType,task_status as TaskStatus,task_name as TaskName,remark as Remark,estimate_execute_time as TaskExecutionTime,create_time as CreateTime,update_time as UpdateTime,completed_time as CompletedTime,task_result_remark as TaskResultRemark,task_result_data as TaskResultData,task_result as TaskResult,short_message as ShortMessage,is_callback_sent as IsCallbackSent,yg_version as HardwareVersion,davinci_version as DavinciVersion,aidl_version as AidlVersion,rom_version as RomVersion,js_version as JsVersion,execution_mode as ExecutionMode FROM task_center.task_details order by task_no LIMIT {(pageNo - 1) * pageSize},{pageSize};");
            //    if (list != null & list.Count() > 0)
            //    {
            //        var bulkIndexResponse = client.Bulk(b => b
            //                .Index("task_details")
            //                .IndexMany(list)
            //                );
            //    }
            //} while (pageNo++ * pageSize < total);

            //Console.WriteLine("ok");
            #endregion

            #region 同步merchant_task表数据
            //IDbConnection connection = new MySqlConnection("");
            //var total = connection.ExecuteScalar<int>("SELECT COUNT(0) FROM task_center.merchant_task;");
            //int pageNo = 1;
            //int pageSize = 500;
            //do
            //{
            //    var list = connection.Query<MerchantTask>($"SELECT serial_no as SerialNo,merchant_no as MerchantNo,account_id as AccountId,device_no as DeviceNo,task_type as TaskType,request_data as RequestData,create_time as CreateTime,completed_time as CompletedTime,task_result_remark as TaskResultRemark,task_result_data as TaskResultData,task_result as TaskResult,short_message as ShortMessage,is_need_callback as IsNeedCallback,callback_rul as CallbackUrl FROM task_center.merchant_task order by serial_no LIMIT {(pageNo - 1) * pageSize},{pageSize};");
            //    if (list != null & list.Count() > 0)
            //    {
            //        var bulkIndexResponse = client.Bulk(b => b
            //                .Index("merchant_task")
            //                .IndexMany(list)
            //                );
            //    }
            //} while (pageNo++ * pageSize < total);

            //Console.WriteLine("ok");
            #endregion

            #region 数组字段数据插入和查询
            //var createIndexResponse = client.Index(new TestArrayField() { IntList = new List<int>() { 1, 2 }, Blogs = new List<Blog>() { new Blog() { City = "ccc" }, new Blog() { City = "dd" } } }, s => s.Index("array-field-test").Id(1));
            //if (createIndexResponse.ApiCall.Success)
            //{

            //}

            //var response = client.Get<TestArrayField>(3, s => s.Index("array-field-test"));
            //if (response.ApiCall.Success)
            //{

            //}
            #endregion

            #region 创建复杂数组字段
            //var createIndexResponse = client.Indices.Create("array-field-test", s =>
            //    s.Map<TestArrayField>(m =>
            //        m.Properties(p =>
            //            p
            //            .Number(n =>
            //                n.Name(n2 => n2.IntList).Type(NumberType.Integer)
            //               )
            //            .Object<Blog>(o =>
            //            o.Name(n2 => n2.Blogs)
            //            .Properties(p2 =>
            //                p2.Keyword(k =>
            //                    k.Name(n2 => n2.City)
            //                )
            //            ))
            //        )
            //    )
            //);

            //if (createIndexResponse.ApiCall.Success)
            //{

            //}
            #endregion

            #region 数组字段数据插入和查询
            ////var createIndexResponse = client.Index(new TestArrayField() { IntList = new List<int>() { 1, 2 } }, s => s.Index("array-field-test").Id(1));
            ////if (createIndexResponse.ApiCall.Success)
            ////{

            ////}

            //var response = client.Get<TestArrayField>(1,s=>s.Index("array-field-test"));
            //if (response.ApiCall.Success)
            //{

            //}
            #endregion

            #region 创建数组字段
            //var createIndexResponse = client.Indices.Create("array-field-test", s =>
            //    s.Map<TestArrayField>(m =>
            //        m.Properties(p =>
            //            p
            //            .Number(n =>
            //                n.Name(n2 => n2.IntList).Type(NumberType.Integer)
            //               )
            //        )
            //    )
            //);

            //if (createIndexResponse.ApiCall.Success)
            //{

            //}
            #endregion

            #region 搜索多字段(fields)
            ////var searchResponse = client.Search<Blog>(s => s.Query(q => q.Match(m => m.Field(f => f.City).Query("york"))).Index("my-index-000001"));//text分词搜索
            //var searchResponse = client.Search<Blog>(s => s.Query(q => q.Match(m => m.Field(f => f.City.Suffix("aa")).Query("York"))).Index("my-index-000001"));//keyword精确搜索
            //if (searchResponse.ApiCall.Success)
            //{

            //}
            #endregion

            #region 创建索引的同时并设置显性映射
            //var createIndexResponse = client.Indices.Create("test", s =>
            //    s.Map<Student>(m =>
            //        m.Properties(p =>
            //            p
            //            .Number(n =>
            //                n.Name(n2 => n2.Id)
            //                  .Type(NumberType.Integer)
            //               )
            //              .Text(t => t
            //                .Name(n2 => n2.Name)
            //              )
            //              .Number(n =>
            //                n.Name(n2 => n2.Age)
            //                  .Type(NumberType.Integer)
            //              )
            //              .Object<Address>(o =>
            //                o.Name(n2 => n2.Address)
            //                .Properties(ap =>
            //                    ap.Keyword(k =>
            //                        k.Name(n2 => n2.Province)
            //                    ).Keyword(k =>
            //                        k.Name(n2 => n2.City)
            //                    )
            //                )
            //              )
            //        )
            //    )
            //);
            //if (createIndexResponse.ApiCall.Success)
            //{

            //}
            #endregion

            #region 查询所有文档
            //var searchResponse = client.Search<Student>(selector => selector.MatchAll().Sort(s => s.Ascending(f => f.Id)).Index("alias-student").From(0).Size(1));
            //if (searchResponse.ApiCall.Success)
            //{
            //    foreach (var doc in searchResponse.Documents)
            //    {
            //        Console.WriteLine($"id：{doc.Id}，name：{doc.Name}，age：{doc.Age}，province：{doc.Address.Province}，city：{doc.Address.City}");
            //    }
            //}
            //Console.WriteLine(searchResponse.Total);
            #endregion

            #region 重新索引文档
            //var reindexResponse = client.ReindexOnServer(r => r
            //    .Source(s => s
            //        .Index("source_index")
            //    )
            //    .Destination(d => d
            //        .Index("destination_index")
            //    )
            //    .WaitForCompletion()
            //);
            #endregion

            #region 创建管道
            //client.Indices.Create("people", c => c
            //    .Map<Person>(p => p
            //        .AutoMap()
            //        .Properties(props => props
            //            .Keyword(t => t.Name("initials"))
            //            .Ip(t => t.Name(dv => dv.IpAddress))
            //            .Object<GeoIp>(t => t.Name(dv => dv.GeoIp))
            //        )
            //    )
            //);
            //client.Ingest.PutPipeline("person-pipeline", p => p
            //    .Processors(ps => ps
            //        .Uppercase<Person>(s => s
            //            .Field(t => t.LastName)
            //        )
            //        .Script(s => s
            //            .Lang("painless")
            //            .Source("ctx.initials = ctx.firstName.substring(0,1) + ctx.lastName.substring(0,1)")
            //        )
            //        .GeoIp<Person>(s => s
            //            .Field(i => i.IpAddress)
            //            .TargetField(i => i.GeoIp)
            //        )
            //    )
            //);
            //var person = new Person()
            //{
            //    Id = 1,
            //    FirstName = "Martijn",
            //    LastName = "Laarman",
            //    IpAddress = "139.130.4.5"
            //};

            //var indexResponse = client.Index(person, p => p.Index("people").Pipeline("person-pipeline"));

            #endregion

            #region 批量添加
            //var people = new[]
            //{
            //    new Person
            //    {
            //        Id=1,
            //        FirstName="Martijn",
            //        LastName="Laarman"
            //    },
            //    new Person
            //    {
            //        Id=2,
            //        FirstName="Stuart",
            //        LastName="Cam"
            //    },
            //    new Person
            //    {
            //        Id=3,
            //        FirstName="Russ",
            //        LastName="Cam"
            //    }
            //};
            //var bulkIndexResponse = client.Bulk(b => b
            //        .Index("people")
            //        .IndexMany(people)
            //        );

            //var bulkIndexResponse2 = client.Bulk(b => b
            //        .Index("people")
            //        .IndexMany(people, (descriptor, person) => descriptor
            //            .Index(person.Id % 2 == 0
            //                ? "even-index"
            //                : "odd-index")
            //            .Pipeline(person.FirstName.StartsWith("M")
            //                ? "startswith_m_pipline"
            //                : "does_not_start_with_m_pipeline")
            //            )
            //        );
            #endregion

            #region 添加多个文档
            //var people = new[]
            //{
            //    new Person
            //    {
            //        Id=1,
            //        FirstName="Martijn",
            //        LastName="Laarman"
            //    },
            //    new Person
            //    {
            //        Id=2,
            //        FirstName="Stuart",
            //        LastName="Cam"
            //    },
            //    new Person
            //    {
            //        Id=3,
            //        FirstName="Russ",
            //        LastName="Cam"
            //    }
            //};

            //var indexManyResponse = client.IndexMany(people);

            //if (indexManyResponse.Errors)
            //{
            //    foreach (var itemWithError in indexManyResponse.ItemsWithErrors)
            //    {
            //        Console.WriteLine($"Failed to index document {itemWithError.Id}：{itemWithError.Error}");
            //    }
            //}

            ////var indexManyAsyncResponse = await client.IndexManyAsync(people);

            #endregion

            #region 添加单个文档
            //var person = new Person()
            //{
            //    Id = 1,
            //    FirstName = "Martijn",
            //    LastName = "Laarman"
            //};
            //var indexResponse1 = client.Index(person, i => i.Index("people"));

            //var indexResponse2 = client.Index(new IndexRequest<Person>(person, "people"));

            #endregion

        }
    }

    public class MerchantTaskParam : PageParam
    {
        /// <summary>
        /// 任务编号
        /// </summary>
        public string SerialNo { get; set; }

        /// <summary>
        /// 商户
        /// </summary>
        public string MerchantNo { get; set; }

        /// <summary>
        /// 账号
        /// </summary>
        public string AccountId { get; set; }

        /// <summary>
        /// 设备编号
        /// </summary>
        public string DeviceNo { get; set; }

        /// <summary>
        /// 短消息
        /// </summary>
        public string ShortMessage { get; set; }

        /// <summary>
        /// 任务类型
        /// </summary>
        public List<string> TaskType { get; set; }

        /// <summary>
        /// 任务是否成功,null:所有，0:还未结束，1：成功，-1：失败，-2：超时
        /// </summary>
        public int? TaskResult { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public DateTime? BeginTime { get; set; }

        /// <summary>
        /// 结束时间
        /// </summary>
        public DateTime? EndTime { get; set; }

        /// <summary>
        /// 任务回调结果
        /// </summary>
        public string TaskResultRemark { get; set; }
    }

    public class PageParam
    {
        /// <summary>
        /// 
        /// </summary>
        public int PageIndex { get; set; } = 1;
        /// <summary>
        /// 
        /// </summary>
        public int PageSize { get; set; } = 10;
    }

    public class MerchantTask
    {
        /// <summary>
        /// 任务序列号
        /// </summary>
        public string SerialNo { get; set; } = string.Empty;

        /// <summary>
        /// 商户编号
        /// </summary>
        public string? MerchantNo { get; set; } = string.Empty;

        /// <summary> 
        /// 账号ID
        /// </summary>
        public string AccountId { get; set; } = string.Empty;

        /// <summary>
        /// 设备编号
        /// </summary>
        public string? DeviceNo { get; set; } = string.Empty;

        /// <summary>
        /// 任务类型 任务类型 提交任务时的参数 需要与业务方提前约定
        /// </summary>
        public string? TaskType { get; set; } = string.Empty;

        /// <summary>
        /// 业务方提交任务时所带的参数
        /// </summary>
        public string? RequestData { get; set; } = string.Empty;

        /// <summary>
        /// 任务创建时间
        /// </summary>
        public DateTime? CreateTime { get; set; } = DateTime.Now;

        /// <summary>
        /// 任务结果时间
        /// </summary>
        public DateTime? CompletedTime { get; set; } = new DateTime(2999, 12, 31, 0, 0, 0);

        /// <summary>
        /// 任务结果说明
        /// </summary>
        public string? TaskResultRemark { get; set; } = string.Empty;

        /// <summary>
        /// 任务结果
        /// </summary>
        public string? TaskResultData { get; set; } = string.Empty;

        /// <summary>
        /// 任务是否成功,0:还未结束，1：成功，-1：失败，-2：超时
        /// </summary>
        public int TaskResult { get; set; }

        /// <summary>
        /// 短消息内容 
        /// </summary>
        public string? ShortMessage { get; set; } = string.Empty;

        /// <summary>
        /// 是否需要回调
        /// </summary>
        public bool IsNeedCallback { get; set; }

        /// <summary>
        /// 回调地址
        /// </summary>
        public string? CallbackUrl { get; set; } = string.Empty;
    }

    public class TaskDetails
    {
        /// <summary>
        /// 任务序列号
        /// </summary>
        public string TaskNo { get; set; } = string.Empty;

        /// <summary>
        /// 任务序列号 由任务中心生成 merchant_task的主键
        /// </summary>
        public string? SerialNo { get; set; } = string.Empty;

        /// <summary>
        /// 商户编号
        /// </summary>
        public string? MerchantNo { get; set; } = string.Empty;

        /// <summary>
        /// 外部系统业务唯一编号
        /// </summary>
        public string? OuterSerialNo { get; set; } = string.Empty;

        /// <summary> 
        /// 账号ID
        /// </summary>
        public string AccountId { get; set; } = string.Empty;

        /// <summary>
        /// 设备编号 根据调度中心开始执行回调更新 (800000)
        /// </summary>
        public string? DeviceNo { get; set; } = string.Empty;

        /// <summary>
        /// js脚本url
        /// </summary>
        public string? JsUrl { get; set; } = string.Empty;

        /// <summary>
        /// 脚本 参数
        /// </summary>
        public string? ParamMap { get; set; } = string.Empty;

        /// <summary>
        /// 资源类型 1-> 微信；2->WhatsApp; 3->淘宝； 4-> 抖音
        /// </summary>
        public int ResourceType { get; set; }

        /// <summary>
        /// 任务状态 10=初始状态，11=已推送 （推送至调度中心更新），12=执行中 （800000），13 = 成功 （800001），14=失败 （800001）
        /// </summary>
        public int TaskStatus { get; set; }

        /// <summary>
        /// 任务名称  与任务类型一一对应
        /// </summary>
        public string? TaskName { get; set; } = string.Empty;

        /// <summary>
        /// 任务说明
        /// </summary>
        public string? Remark { get; set; } = string.Empty;

        /// <summary>
        /// 预计执行时间(ms为单位)
        /// </summary>
        public long TaskExecutionTime { get; set; }

        /// <summary>
        /// 任务创建时间
        /// </summary>
        public DateTime? CreateTime { get; set; } = DateTime.Now;

        /// <summary>
        /// 任务更新时间
        /// </summary>
        public DateTime? UpdateTime { get; set; } = DateTime.Now;

        /// <summary>
        /// 任务结果时间
        /// </summary>
        public DateTime? CompletedTime { get; set; } = new DateTime(2999, 12, 31, 0, 0, 0);

        /// <summary>
        /// 任务结果说明   800003回调回来的的message 
        /// </summary>
        public string? TaskResultRemark { get; set; } = string.Empty;

        /// <summary>
        /// 任务结果  800003回调回来的的data 
        /// </summary>
        public string? TaskResultData { get; set; } = string.Empty;

        /// <summary>
        /// 短消息内容 
        /// </summary>
        public string? ShortMessage { get; set; } = string.Empty;

        /// <summary>
        /// 任务是否成功,0:还未结束，1：成功，-1：失败，-2：超时
        /// </summary>
        public int TaskResult { get; set; }

        /// <summary>
        /// 回调地址
        /// </summary>
        public bool IsCallbackSent { get; set; }

        /// <summary>
        /// 硬改版本号
        /// </summary>
        public string? HardwareVersion { get; set; } = string.Empty;

        /// <summary>
        /// 达芬奇版本号
        /// </summary>
        public string? DavinciVersion { get; set; } = string.Empty;

        /// <summary>
        /// Aidl 版本号
        /// </summary>
        public string? AidlVersion { get; set; } = string.Empty;

        /// <summary>
        /// Rom 版本号
        /// </summary>
        public string? RomVersion { get; set; } = string.Empty;

        /// <summary>
        /// js脚本版本号
        /// </summary>
        public int? JsVersion { get; set; } = 0;

        /// <summary>
        /// 执行方式，1：云机调度，2：无障碍DB
        /// </summary>
        public int? ExecutionMode { get; set; } = 0;
    }

    public class Person
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string IpAddress { get; set; }
        public GeoIp GeoIp { get; set; }
    }

    public class GeoIp
    {
        public string CityName { get; set; }
        public string ContinentName { get; set; }
        public string CountryIsoCode { get; set; }
        public GeoLocation Location { get; set; }
        public string RegionName { get; set; }
    }

    public class Student
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public Address Address { get; set; }
    }

    public class Address
    {
        public string Province { get; set; }
        public string City { get; set; }
    }

    public class Blog
    {
        public string City { get; set; }
    }

    public class TestArrayField
    {
        public List<int> IntList { get; set; }
        public List<Blog> Blogs { get; set; }
    }

}
