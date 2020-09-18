using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQL2AEH
{
    public static class IEnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Split<T>(this IEnumerable<T> list, int partsSize)
        {
            return list.Select((item, index) => new { index, item })
                       .GroupBy(x => x.index / partsSize)
                       .Select(x => x.Select(y => y.item));
        }
    }

    public class SqlTextQuery
    {
        private readonly string _sqlDatabaseConnectionString;

        public SqlTextQuery(string sqlDatabaseConnectionString)
        {
            _sqlDatabaseConnectionString = sqlDatabaseConnectionString;
        }

        public IEnumerable<Dictionary<string, object>> PerformQuery(string query)
        {
            var command = new SqlCommand(query);

            IEnumerable<Dictionary<string, object>> result = null;
            using (var sqlConnection = new SqlConnection(_sqlDatabaseConnectionString))
            {
                sqlConnection.Open();

                command.Connection = sqlConnection;
                using (SqlDataReader r = command.ExecuteReader())
                {
                    result = Serialize(r);
                }
                sqlConnection.Close();
            }
            return result;
        }

        private IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            while (reader.Read())
            {
                var row = new Dictionary<string, object>();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row.Add(reader.GetName(i), reader.GetValue(i));
                }
                results.Add(row);
            }
            return results;
        }
    }

    class Program
    {
        static void Main()
        {
            try
            {
                Console.WriteLine($"Started at {DateTime.Now}");

                // CONFIGURABLE VARIABLES
                int SQLBatchSize = Convert.ToInt32(ConfigurationManager.AppSettings["SQLBatchSize"]);
                int ExecutionControl = Convert.ToInt32(ConfigurationManager.AppSettings["ExecutionControl"]);
                int ExecutionControlSleepMs = Convert.ToInt32(ConfigurationManager.AppSettings["ExecutionControlSleepMs"]);

                // VARIABLES
                string updateOffsetQuery;
                string selectOffsetQuery;
                string offsetString;
                string selectDataQuery;
                string nextOffset;

                // GET SQL & HUB CONNECTION STRINGS
                string sqlDatabaseConnectionString = ConfigurationManager.AppSettings["sqlDatabaseConnectionString"];
                string serviceBusConnectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ServiceBusConnectionString"];
                string hubName = ConfigurationManager.AppSettings["Microsoft.ServiceBus.EventHubToUse"];

                // GET SQL SERVER SOURCE TABLE
                var dataTableNames = ConfigurationManager.AppSettings["DataTableName"].Split(',').ToList();
                
                // ESTABLISH SQL & HUB CONNECTIONS
                SqlTextQuery queryPerformer = new SqlTextQuery(sqlDatabaseConnectionString);
                EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(serviceBusConnectionString, hubName);

                do
                {
                    foreach (var table in dataTableNames)
                    {
                        try
                        {
                            // GET CURRENT OFFSET FOR SQL TABLE
                            selectOffsetQuery = "select convert(nvarchar(100), LastMaxVal, 1) as stringLastMaxVal from dbo.SQL2AEH_TableOffset where TableName = '" + table + "'";
                            Dictionary<string, object> offsetQueryResult = queryPerformer.PerformQuery(selectOffsetQuery).FirstOrDefault();
                            offsetString = offsetQueryResult.Values.First().ToString();

                            // GET ROWS FROM SQL CDC CHANGE TRACKING TABLE GREATER THAN THE OFFSET
                            selectDataQuery = "select '" + table + "' as SQL2AEH_TableName, ROW_NUMBER() OVER (ORDER BY [__$start_lsn]) as SQL2AEH_RowNbr, convert(nvarchar(100), __$start_lsn, 1) as SQL2AEH_$start_lsn_string, convert(nvarchar(100), __$seqval, 1) as SQL2AEH_$seqval_string, convert(nvarchar(100), __$update_mask, 1) as SQL2AEH_$update_mask_string, * from " + TableToCdcTableName(table) + " where __$start_lsn > " + offsetString + " order by __$start_lsn";
                            IEnumerable<Dictionary<string, object>> resultCollection = queryPerformer.PerformQuery(selectDataQuery);

                            // IF THERE ARE NEW ROWS TO SEND...
                            if (resultCollection.Any())
                            {
                                IEnumerable<Dictionary<string, object>> orderedByColumnName = resultCollection.OrderBy(r => r["SQL2AEH_RowNbr"]);

                                // GROUP ROWS TO SEND INTO A MESSAGE BATCH
                                foreach (var resultGroup in orderedByColumnName.Split(SQLBatchSize))
                                {
                                    // SEND BATCH ROWS TO EVENT HUB AS JSON MESSAGE
                                    Console.WriteLine($"sending message to eventhub {DateTime.UtcNow}");
                                    SendRowsToEventHub(eventHubClient, resultGroup).Wait();

                                    // UPDATE CURRENT VALUE IN SQL TABLE OFFSET
                                    nextOffset = resultGroup.Max(r => r["SQL2AEH_$start_lsn_string"].ToString());
                                    updateOffsetQuery = "update dbo.SQL2AEH_TableOffset set LastMaxVal = convert(binary(10), '" + nextOffset + "', 1), LastUpdateDateTime = getdate() where TableName = '" + table + "'";
                                    queryPerformer.PerformQuery(updateOffsetQuery);
                                }
                            }

                            // UPDATE OFFSET LAST CHECKED DATE
                            updateOffsetQuery = "update dbo.SQL2AEH_TableOffset set LastCheckedDateTime = getdate() where TableName = '" + table + "'";
                            queryPerformer.PerformQuery(updateOffsetQuery);

                            // WAIT BEFORE ITERATING LOOP
                            Thread.Sleep(ExecutionControlSleepMs);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Inner Loop: {ex.Message}");
                            Thread.Sleep(2000);
                        }
                    }
                } while (ExecutionControl == 1); // LOOP IF RUN CONTINUOUS ENABLED
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Thread.Sleep(2000);
            }
        }

        private static string TableToCdcTableName(string tableName)
        {
            return "cdc." + tableName.Replace(".", "_") + "_CT";
        }

        private static async Task SendRowsToEventHub(EventHubClient eventHubClient, IEnumerable<object> rows)
        {  
            var memoryStream = new MemoryStream();

            using (var sw = new StreamWriter(memoryStream, new UTF8Encoding(false), 1024, leaveOpen: true))
            {
                string serialized = JsonConvert.SerializeObject(rows);
                sw.Write(serialized);
                sw.Flush();
            }

            Debug.Assert(memoryStream.Position > 0, "memoryStream.Position > 0");

            memoryStream.Position = 0;
            EventData eventData = new EventData(memoryStream);

            await eventHubClient.SendAsync(eventData);
        }
    }
}
