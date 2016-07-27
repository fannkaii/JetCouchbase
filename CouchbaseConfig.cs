using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.Management;
using System;
using System.Collections.Generic;

namespace JetCouchbase
{
    public static class CouchbaseConfig
    {
        public static void Initialize()
        {
            var config = new ClientConfiguration();
            config.BucketConfigs.Clear();
            config.Servers = new List<Uri>(new Uri[] { new Uri(DbConfig.Instance.Server) });
            config.UseSsl = false;
            config.DefaultOperationLifespan = 1000;

            config.BucketConfigs.Add(
                DbConfig.Instance.Bucket,
                new BucketConfiguration
                {
                    BucketName = DbConfig.Instance.Bucket,
                    Username = DbConfig.Instance.User,
                    Password = DbConfig.Instance.Password,
                    UseSsl = false,
                    PoolConfiguration = new PoolConfiguration { MaxSize = 10, MinSize = 5, SendTimeout = 12000 },
                    Servers = config.Servers
                });

            ClusterHelper.Initialize(config);

            IBucket bucket = ClusterHelper.GetBucket(DbConfig.Instance.Bucket);
            IBucketManager manager = bucket.CreateManager(DbConfig.Instance.User, DbConfig.Instance.Password);

            //Creates the primary index for the current bucket if it doesn't already exist.
            manager.CreateN1qlPrimaryIndexAsync(false);
        }

        public static void Close()
        {
            ClusterHelper.Close();
        }
    }
}
