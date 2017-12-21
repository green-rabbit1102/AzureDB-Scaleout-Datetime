using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Client;
using Microsoft.Azure.SqlDatabase.ElasticScale.SplitMerge.Worker;

namespace SplitMergeConsole
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            // 引数用の変数の宣言
            string UserName;
            string Password;
            string SplitDate;

            //  引数の確認
            if (args.Length != 3) {
                Console.WriteLine("Expected UserName and Password arguments.");
                Console.WriteLine("引数の指定が正しくありません。");
                Console.WriteLine("ユーザ名・パスワード・分割を行う日付を指定してください。");
                Environment.Exit(1);
            }

            // 変数に格納
            UserName    = args[0];
            Password    = args[1];
            SplitDate   = args[2];

            // 日付を「DateTime」型に変換
            DateTime splitValue = DateTime.Parse(SplitDate);

            // 実行メソッド
            DoSplit(UserName, Password, splitValue);
        }


        // スケールアウト実行メソッド
        private static void DoSplit<T>(string UserName, string Password, T splitValue)
        {
            string ShardMapManagerServerName    = ".";                          // SQLサーバ名
            string ShardMapManagerDatabaseName  = "SplitMergeShardManagement";  // ShardMapManagerのDB名
            string ShardServerName1             = ShardMapManagerServerName;    // スケールアウト元のSQLサーバ名
            string ShardDatabaseName1           = "ShardDb1";                   // スケールアウト元のDB名
            string ShardServerName2             = ShardMapManagerServerName;    // スケールアウト先のSQLサーバ名
            string ShardDatabaseName2           = "ShardDb2";                   // スケールアウト先のDB名

            string ShardMapName                 = "MyTestShardMap";             // SharMap名
            string SplitMergeServerName         = ShardMapManagerServerName;    // スケールアウトの情報を登録するSQLサーバ名
            string SplitMergeDatabaseName       = ShardMapManagerDatabaseName;  // スケールアウトの情報を登録するDB名


            // Get shard map
            string smmConnStr = new SqlConnectionStringBuilder
            {
                UserID = UserName,
                Password = Password,
                DataSource = ShardMapManagerServerName,
                InitialCatalog = ShardMapManagerDatabaseName
            }.ToString();

            string shardConnStr = new SqlConnectionStringBuilder
            {
                UserID = UserName,
                Password = Password,
            }.ToString();

            ShardMapManager smm = ShardMapManagerFactory.GetSqlShardMapManager(
                smmConnStr,
                ShardMapManagerLoadPolicy.Lazy);

            RangeShardMap<T> rsm = smm.GetRangeShardMap<T>(ShardMapName);

            // スケールアウトしたデータを戻す場合
            // Shard targetShard = rsm.GetShard(new ShardLocation(ShardServerName1, ShardDatabaseName1));
            Shard targetShard = rsm.GetShard(new ShardLocation(ShardServerName2, ShardDatabaseName2));

            // Split-merge worker config
            string certFile = "testCert.pfx";
            string certFilePassword = "password";

            X509Certificate2 encryptionCert = new X509Certificate2();
            encryptionCert.Import(certFile, certFilePassword, X509KeyStorageFlags.DefaultKeySet);

            string splitMergeConnStr = new SqlConnectionStringBuilder
            {
                UserID = UserName,
                Password = Password,
                DataSource = SplitMergeServerName,
                InitialCatalog = SplitMergeDatabaseName
            }.ToString();

            // スケールアウトの実行
            using (SplitMergeWorker worker = SplitMergeWorkerFactory.CreateSplitMergeWorker(splitMergeConnStr, encryptionCert))
            {
                SplitMergeOperation op = worker.Split(
                    smmConnStr,
                    shardConnStr,
                    rsm,
                    splitValue,
                    targetShard,
                    SplitMergePolicy.MoveLowerKeySegment);  // 指定した日付以前のデータをスケールアウトする設定

                worker.WaitForOperationToFinish(op);
            }
        }
}
}
