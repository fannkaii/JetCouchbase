using Couchbase;
using Couchbase.N1QL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace JetCouchbase
{
    public class JetN1QL_Common
    {
        /// <summary>
        /// 执行N1QL语句的超时时间（单位：秒）
        /// </summary>
        private double _timeout;
        /// <summary>
        /// 执行N1QL语句的超时时间（单位：秒）
        /// </summary>
        public double Timeout { get { return this._timeout; } set { this._timeout = value; } }

        /// <summary>
        /// 构造函数
        /// </summary>
        public JetN1QL_Common()
        {
            this._timeout = DbConfig.Instance.Timeout;
        }

        #region 基本增删改查操作
        /// <summary>
        /// 判断某文档是否已存在
        /// </summary>
        /// <param name="docId">文档id</param>
        /// <returns></returns>
        public bool Exists(string docId)
        {
            return ClusterHelper.GetBucket(DbConfig.Instance.Bucket).Exists(docId);
        }

        // 将对象添加到数据库中
        /// <summary>
        /// 将对象添加到数据库中
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="doc">类型为T的对象</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>操作是否成功</returns>
        public bool Add<T>(T doc, out string docid, out string message) where T : class
        {
            Type type = typeof(T);

            //根据文档类型生成一个文档的编号Id，由类型全名和base64编码后的guid组成
            string base64id = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).TrimEnd('=').Replace("+", "@").Replace("/", "=");
            string newid = type.FullName + "-" + base64id;

            //以下判断doc是否有常见的三个id属性，如有，则将其设为和文档编号相同（方便在应用程序中的相关操作）
            PropertyInfo pi;
            pi = type.GetProperty("id"); if (pi != null) pi.SetValue(doc, newid);
            pi = type.GetProperty("Id"); if (pi != null) pi.SetValue(doc, newid);
            pi = type.GetProperty("ID"); if (pi != null) pi.SetValue(doc, newid);
            pi = type.GetProperty("TypeClassName"); if (pi != null) pi.SetValue(doc, type.FullName);


            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //调用数据格的插入方法来新建记录
            IOperationResult<T> r = bucket.Insert<T>(newid, doc);

            //如果操作成功则message返回生成的id
            if (r.Success)
            {
                docid = newid;
                message = newid;
            }
            else//不成功则message返回具体信息
            {
                docid = string.Empty;
                message = r.Message;
                if (r.Exception != null) message += " Exception:" + r.Exception.Message;
            }

            //返回是否操作成功
            return r.Success;
        }

        // 将对象添加到数据库中,使用指定的编号id
        /// <summary>
        /// 将对象添加到数据库中,使用指定的编号id
        /// </summary>
        /// <param name="newid">指定的编号id</param>
        /// <param name="doc">类型为T的对象</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>操作是否成功</returns>
        public bool AddRaw(string newid, dynamic doc, out string message)
        {
            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //如果数据格中已存在此编号的记录，则message返回错误信息提示此编号的文档已存在，方法返回false
            if (bucket.Exists(newid))
            {
                message = "Error: doc with id: " + newid + " already exits, use Update method instead!";
                return false;
            }

            //调用数据格的插入方法来新建记录
            IOperationResult<dynamic> r = bucket.Insert<dynamic>(newid, doc);

            //如果操作成功则message返回生成的id
            if (r.Success)
            {
                message = newid;
            }
            else//不成功则message返回具体信息
            {
                message = r.Message;
                if (r.Exception != null) message += " Exception:" + r.Exception.Message;
            }

            //返回是否操作成功
            return r.Success;
        }

        // 从数据格中删除记录（根据编号id）
        /// <summary>
        /// 从数据格中删除记录（根据编号id）
        /// </summary>
        /// <param name="id">编号id</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>操作是否成功</returns>
        public bool Delete(string id, out string message)
        {
            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //调用数据格的移除方法来删除记录
            IOperationResult r = bucket.Remove(id);

            //message返回具体信息
            message = r.Message;
            if (r.Exception != null) message += " Exception:" + r.Exception.Message;

            //返回是否操作成功
            return r.Success;
        }

        // 从数据格中更新记录（根据编号id）
        /// <summary>
        /// 从数据格中更新记录（根据编号id）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="id">编号id</param>
        /// <param name="doc">类型为T的对象</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>操作是否成功</returns>
        public bool Update<T>(string id, T doc, out string message) where T : class
        {
            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //如果数据格中存在此编号的记录，则继续操作
            if (bucket.Get<T>(id).Success)
            {
                //以下判断doc是否有常见的三个id属性，如有，则将其设为和文档编号相同（方便在应用程序中的相关操作）
                PropertyInfo pi;
                Type type = typeof(T);

                pi = type.GetProperty("id"); if (pi != null) pi.SetValue(doc, id);
                pi = type.GetProperty("Id"); if (pi != null) pi.SetValue(doc, id);
                pi = type.GetProperty("ID"); if (pi != null) pi.SetValue(doc, id);
                pi = type.GetProperty("TypeClassName"); if (pi != null) pi.SetValue(doc, type.FullName);

                //调用数据格的"更新及插入"方法来更新记录
                IOperationResult<T> r = bucket.Upsert<T>(id, doc);

                //message返回具体信息
                message = r.Message;
                if (r.Exception != null) message += " Exception:" + r.Exception.Message;

                //返回是否操作成功
                return r.Success;
            }
            else//反之则message返回错误信息提示此编号的文档不存在，方法返回false
            {
                message = "Error: doc with id: " + id + " does not exit, use Add method instead!";
                return false;
            }
        }

        // 从数据格中获取记录（根据编号id）
        /// <summary>
        /// 从数据格中获取记录（根据编号id）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="id">编号id</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>类型为T的对象</returns>
        public T Get<T>(string id, out string message) where T : class
        {
            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //调用数据格的"Get"方法来更新记录
            IOperationResult<T> r = bucket.Get<T>(id);

            //message返回具体信息
            message = r.Message;
            if (r.Exception != null) message += " Exception:" + r.Exception.Message;

            //如果操作成功则返回具体的结果值
            if (r.Success)
            {
                return r.Value;
            }
            else//反之则返回null
            {
                return null;
            }
        }

        // 从数据格中获取记录List（根据编号id列表）
        /// <summary>
        /// 从数据格中获取记录List（根据编号id列表）
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="ids">编号id列表</param>
        /// <param name="message">操作过程中反馈的信息</param>
        /// <returns>类型为T的对象列表</returns>
        public List<T> Get<T>(IList<string> ids, out string message) where T : class
        {
            if (ids == null || ids.Count == 0) { message = "ids error"; return null; }

            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            //调用数据格的"Get"方法来更新记录
            IDictionary<string, IOperationResult<T>> rs = bucket.Get<T>(ids);

            var rssc = 0;
            var L = new List<T> { };

            message = "{\"Title\":\"ResultMessages\"";

            foreach (var r in rs)
            {
                message += ",\"" + r.Key + "\":\"" + r.Value.Message;
                if (r.Value.Exception != null)
                {
                    message += "Exception:" + r.Value.Exception.Message;
                }
                message += "\"";

                //如果操作成功则
                if (r.Value.Success)
                {
                    rssc++;
                    L.Add(r.Value.Value);
                }
            }

            return L;

        }
        #endregion

        /// <summary>
        /// 更新N1QL查询结果集内的文档——文档的类型内含TypeClassName属性，且该属性无值的文档
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="lstRow">类型为T的查询结果列表</param>
        /// <returns>更新的记录数</returns>
        public UInt32 UpsertCategoryFieldForDocument<T>(List<T> lstRow)
        {
            UInt32 mutationCount = 0;
            if (lstRow != null && lstRow.Count > 0)
            {
                Type type = typeof(T);
                if (type.ToString() != "System.Object") //必须是强类型，查询结果集的类型是弱类型时，此方法无效
                {
                    IList<string> lstId = new List<string>();
                    foreach (T doc in lstRow)
                    {
                        PropertyInfo pi_Id = type.GetProperty("Id");
                        PropertyInfo pi_Category = type.GetProperty("TypeClassName");
                        if (pi_Id != null && pi_Category != null)
                        {
                            object objCategory = pi_Category.GetValue(doc);
                            if (objCategory == null || objCategory.ToString().Length == 0)
                            {
                                string strId = pi_Id.GetValue(doc).ToString();
                                if (!string.IsNullOrEmpty(strId)) lstId.Add(strId);
                            }
                        }
                    }

                    if (lstId.Count > 0)
                    {
                        string strIds = "'" + string.Join<string>("','", lstId) + "'";
                        string strN1QL = string.Format("UPDATE `{0}` USE KEYS[{1}] SET TypeClassName = SUBSTR(Id, 0, POSITION(Id, '-'))", DbConfig.Instance.Bucket, strIds);
                        string message = string.Empty;
                        mutationCount = JetN1QL.Instance.RunNonQuery(out message, strN1QL);
                    }

                }
            }
            return mutationCount;
        }

        #region 中间方法
        /// <summary>
        /// 返回N1QL执行后的相关信息
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="queryResult">N1QL的执行结果对象</param>
        /// <returns>N1QL执行后的相关信息</returns>
        protected string ReturnQueryInfo<T>(IQueryResult<T> queryResult)
        {
            string message = string.Empty;

            if (!string.IsNullOrEmpty(queryResult.Message)) message = queryResult.Message;

            if (!queryResult.Success)
            {
                if (queryResult.Errors != null && queryResult.Errors.Count > 0)
                {
                    IList<string> lst = new List<string>();
                    foreach (Error err in queryResult.Errors) lst.Add(err.Message);
                    message += " Error：" + string.Join<string>(", ", lst);
                }
                else if (queryResult.Exception != null) message += " Exception：" + queryResult.Exception.Message;
                else message += "Unknown Error";
            }

            return message;
        }

        /// <summary>
        /// 根据必要参数生成一个N1QL查询的请求对象
        /// </summary>
        /// <param name="query">N1QL语句</param>
        /// <param name="typeNum">参数类型标识</param>
        /// <param name="parameters_type1">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="parameters_type2">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>N1QL查询的请求对象</returns>
        protected IQueryRequest GetQueryRequest(string query, string typeNum, IDictionary<string, object> parameters_type1, object[] parameters_type2, ScanConsistency scanConsistency, MutationState mutationState)
        {
            IQueryRequest queryRequest = new QueryRequest(query);
            switch (typeNum)
            {
                case "1":
                    if (parameters_type1 != null) queryRequest = queryRequest.AddNamedParameter(parameters_type1.ToArray());
                    break;
                case "2":
                    if (parameters_type2 != null) queryRequest = queryRequest.AddPositionalParameter(parameters_type2);
                    break;
            }

            if (mutationState != null)
            {
                //使用一致性：AtPlus，但实际上无需设置scanConsistency参数为AtPlus，可参考：http://blog.couchbase.com/2016/june/new-to-couchbase-4.5-atplus
                MutationState ms = MutationState.From(mutationState);
                queryRequest = queryRequest.ConsistentWith(ms);
            }
            else queryRequest = queryRequest.ScanConsistency(scanConsistency);

            queryRequest = queryRequest.Timeout(TimeSpan.FromSeconds(this.Timeout));

            return queryRequest;
        }

        /// <summary>
        /// 执行一次N1QL
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="typeNum">参数类型标识</param>
        /// <param name="parameters_type1">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="parameters_type2">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>N1QL查询的请求对象</returns>
        protected IQueryResult<T> ExeN1QL<T>(out string message, string query, string typeNum, IDictionary<string, object> parameters_type1, object[] parameters_type2, ScanConsistency scanConsistency, MutationState mutationState)
        {
            IQueryResult<T> queryResult = null;

            var bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);

            IQueryRequest queryRequest = null;
            switch (typeNum)
            {
                case "1":
                    queryRequest = GetQueryRequest(query, "1", parameters_type1, null, scanConsistency, mutationState);
                    break;
                case "2":
                    queryRequest = GetQueryRequest(query, "2", null, parameters_type2, scanConsistency, mutationState);
                    break;
            }

            IQueryResult<T> result = bucket.Query<T>(queryRequest);
            message = this.ReturnQueryInfo(result);
            if (result.Success) queryResult = result;

            return queryResult;
        }

        /// <summary>
        /// 获取查询结果集中的首行首列的值
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="typeNum">参数类型标识</param>
        /// <param name="parameters_type1">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="parameters_type2">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>查询结果集中的首行首列的值(string类型)</returns>
        protected string ExeN1QL_ForGetUniqueValue(out string message, string query, string typeNum, IDictionary<string, object> parameters_type1, object[] parameters_type2, ScanConsistency scanConsistency, MutationState mutationState)
        {
            string str = string.Empty;
            message = string.Empty;

            query = "select t.* from (" + query + ") t limit 1";

            IQueryResult<dynamic> queryResult = null;

            switch (typeNum)
            {
                case "1":
                    queryResult = this.ExeN1QL<dynamic>(out message, query, "1", parameters_type1, null, scanConsistency, mutationState);
                    break;
                case "2":
                    queryResult = this.ExeN1QL<dynamic>(out message, query, "2", null, parameters_type2, scanConsistency, mutationState);
                    break;
            }

            if (queryResult != null)
            {
                dynamic re = queryResult.Rows.First<dynamic>();
                str = ((Newtonsoft.Json.Linq.JProperty)((Newtonsoft.Json.Linq.JContainer)re).First).Value.ToString();
            }

            return str;
        }

        /// <summary>
        /// 获取查询结果集的分页数据
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="curPageNo">当前页码(页码约定为从1开始)</param>
        /// <param name="pageSize">前端列表每页显示的记录数量</param>
        /// <param name="typeNum">参数类型标识</param>
        /// <param name="parameters_type1">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="parameters_type2">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>类型为T的查询结果列表,仅含当前页的数据</returns>
        protected List<T> ExeN1QL_ForGetPagingData<T>(out string message, string query, int curPageNo, int pageSize, string typeNum, IDictionary<string, object> parameters_type1, object[] parameters_type2, ScanConsistency scanConsistency, MutationState mutationState)
        {
            List<T> lstRow = null;
            message = string.Empty;

            query = query + " LIMIT " + pageSize + " OFFSET" + (pageSize * (curPageNo - 1));

            IQueryResult<T> queryResult = null;
            switch (typeNum)
            {
                case "1":
                    queryResult = this.ExeN1QL<T>(out message, query, "1", parameters_type1, null, scanConsistency, mutationState);
                    break;
                case "2":
                    queryResult = this.ExeN1QL<T>(out message, query, "2", null, parameters_type2, scanConsistency, mutationState);
                    break;
            }

            if (queryResult != null) lstRow = queryResult.Rows;

            return lstRow;
        }
        #endregion 中间方法

    }

    public class JetN1QL : JetN1QL_Common
    {
        /// <summary>
        /// 构造函数
        /// </summary>
        public JetN1QL() : base() { }

        private static JetN1QL instance = null;

        public static JetN1QL Instance
        {
            get { if (instance == null) instance = new JetN1QL(); return instance; }
        }

        #region 公开的方法
        /// <summary>
        /// 执行一次N1QL查询(使用字典类型参数)
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>类型为T的查询结果列表</returns>
        public List<T> RunQuery<T>(out string message, string query, IDictionary<string, object> parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            List<T> lstRow = null;

            IQueryResult<T> queryResult = this.ExeN1QL<T>(out message, query, "1", parameters, null, scanConsistency, mutationState);
            if (queryResult != null)
            {
                lstRow = queryResult.Rows;
            }

            return lstRow;
        }

        /// <summary>
        /// 执行一次N1QL查询(使用字典类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>返回查询结果列表的JSON字符串</returns>
        public string RunQuery(out string message, string query, IDictionary<string, object> parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            string strQueryResult = null;

            IQueryResult<dynamic> queryResult = this.ExeN1QL<dynamic>(out message, query, "1", parameters, null, scanConsistency, mutationState);
            if (queryResult != null)
            {
                strQueryResult = Newtonsoft.Json.JsonConvert.SerializeObject(queryResult.Rows);
            }

            return strQueryResult;
        }

        /// <summary>
        /// 执行一次N1QL非查询(使用字典类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">字典类型参数,与N1QL语句搭配使用</param>
        /// <returns>执行一次N1QL非查询后，受影响的行数</returns>
        public UInt32 RunNonQuery(out string message, string query, IDictionary<string, object> parameters = null)
        {
            UInt32 mutationCount = 0;

            IQueryResult<dynamic> queryResult = this.ExeN1QL<dynamic>(out message, query, "1", parameters, null, ScanConsistency.NotBounded, null);
            if (queryResult != null)
            {
                mutationCount = queryResult.Metrics.MutationCount;
            }

            return mutationCount;
        }

        /// <summary>
        /// 获取查询结果集中的首行首列的值(使用字典类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>查询结果集中的首行首列的值(string类型)</returns>
        public string GetUniqueValue(out string message, string query, IDictionary<string, object> parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            string str = this.ExeN1QL_ForGetUniqueValue(out message, query, "1", parameters, null, scanConsistency, mutationState);
            return str;
        }

        /// <summary>
        /// 获取查询结果集的分页数据(使用字典类型参数)
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="curPageNo">当前页码(页码约定为从1开始)</param>
        /// <param name="pageSize">前端列表每页显示的记录数量</param>
        /// <param name="parameters">字典类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>类型为T的查询结果列表,仅含当前页的数据</returns>
        public List<T> GetPagingData<T>(out string message, string query, int curPageNo, int pageSize, IDictionary<string, object> parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            List<T> lstRow = this.ExeN1QL_ForGetPagingData<T>(out message, query, curPageNo, pageSize, "1", parameters, null, scanConsistency, mutationState);
            return lstRow;
        }
        #endregion 公开的方法
    }

    public class JetN1QLII : JetN1QL_Common
    {
        public JetN1QLII() : base() { }

        private static JetN1QLII instance = null;

        public static JetN1QLII Instance
        {
            get { if (instance == null) instance = new JetN1QLII(); return instance; }
        }

        #region 公开的方法
        /// <summary>
        /// 执行一次N1QL查询(使用数组类型参数)
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>类型为T的查询结果列表</returns>
        public List<T> RunQuery<T>(out string message, string query, object[] parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            List<T> lstRow = null;

            IQueryResult<T> queryResult = this.ExeN1QL<T>(out message, query, "2", null, parameters, scanConsistency, mutationState);
            if (queryResult != null) lstRow = queryResult.Rows;

            return lstRow;
        }

        /// <summary>
        /// 执行一次N1QL查询(使用数组类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>返回查询结果列表的JSON字符串</returns>
        public string RunQuery(out string message, string query, object[] parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            string strQueryResult = null;

            IQueryResult<dynamic> queryResult = this.ExeN1QL<dynamic>(out message, query, "2", null, parameters, scanConsistency, mutationState);
            if (queryResult != null)
            {
                strQueryResult = Newtonsoft.Json.JsonConvert.SerializeObject(queryResult.Rows);
            }

            return strQueryResult;
        }

        /// <summary>
        /// 执行一次N1QL非查询(使用数组类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">数组类型参数,与N1QL语句搭配使用</param>
        /// <returns>执行一次N1QL非查询后，受影响的行数</returns>
        public UInt32 RunNonQuery(out string message, string query, object[] parameters = null)
        {
            UInt32 mutationCount = 0;

            IQueryResult<dynamic> queryResult = this.ExeN1QL<dynamic>(out message, query, "2", null, parameters, ScanConsistency.NotBounded, null);
            if (queryResult != null) mutationCount = queryResult.Metrics.MutationCount;

            return mutationCount;
        }

        /// <summary>
        /// 获取查询结果集中的首行首列的值(使用数组类型参数)
        /// </summary>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="parameters">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>查询结果集中的首行首列的值(string类型)</returns>
        public string GetUniqueValue(out string message, string query, object[] parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            string str = this.ExeN1QL_ForGetUniqueValue(out message, query, "2", null, parameters, scanConsistency, mutationState);
            return str;
        }

        /// <summary>
        /// 获取查询结果集的分页数据(使用数组类型参数)
        /// </summary>
        /// <typeparam name="T">类型名</typeparam>
        /// <param name="message">输出N1QL执行后的相关信息</param>
        /// <param name="query">N1QL语句</param>
        /// <param name="curPageNo">当前页码(页码约定为从1开始)</param>
        /// <param name="pageSize">前端列表每页显示的记录数量</param>
        /// <param name="parameters">数组类型参数,与N1QL语句搭配使用</param>
        /// <param name="scanConsistency">索引一致性选项</param>
        /// <param name="mutationState">数据变化状态(使用索引一致性选项AtPlus时设置)</param>
        /// <returns>类型为T的查询结果列表,仅含当前页的数据</returns>
        public List<T> GetPagingData<T>(out string message, string query, int curPageNo, int pageSize, object[] parameters = null, ScanConsistency scanConsistency = ScanConsistency.NotBounded, MutationState mutationState = null)
        {
            List<T> lstRow = this.ExeN1QL_ForGetPagingData<T>(out message, query, curPageNo, pageSize, "2", null, parameters, scanConsistency, mutationState);
            return lstRow;
        }
        #endregion 公开的方法

    }
}
