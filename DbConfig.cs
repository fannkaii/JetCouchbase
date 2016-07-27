using System.Configuration;

namespace JetCouchbase
{
    public class DbConfig
    {
        public DbConfig() { }

        private static DbConfig instance = null;
        public static DbConfig Instance
        {
            get { if (instance == null) { instance = new DbConfig(); } return instance; }
        }

        public string Bucket { get { return ConfigurationManager.AppSettings["couchbaseBucketName"]; } }
        public string Server { get { return ConfigurationManager.AppSettings["couchbaseServer"]; } }
        public string Password { get { return ConfigurationManager.AppSettings["couchbasePassword"]; } }
        public string User { get { return ConfigurationManager.AppSettings["couchbaseUser"]; } }
        public double Timeout = double.Parse(ConfigurationManager.AppSettings["couchbaseN1QLTimeout"]);
    }
}
