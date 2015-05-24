using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;

namespace WebDataReplication
{
    public class StorageAccountMigrator
    {
        private readonly CloudStorageAccount sourceAccount;
        private readonly CloudStorageAccount targetAccount;


        public StorageAccountMigrator()
        {
            var sourceCs = CloudConfigurationManager.GetSetting("DataConnectionSource");
            sourceAccount = CloudStorageAccount.Parse(sourceCs);

            var targetCs = CloudConfigurationManager.GetSetting("DataConnectionTarget");
            targetAccount = CloudStorageAccount.Parse(targetCs);
        }


        public async Task<string> Start()
        {
            return await Task.Run(() => ExecuteMigration());
        }


        private String ExecuteMigration()
        {
            var migrateBlobs = CloudConfigurationManager
                .GetSetting("MigrateBlobs") == "true";

            var migrateTables = CloudConfigurationManager
                .GetSetting("MigrateTables") == "true";

            var tasks = new[]
            {
                migrateBlobs
                    ? MigrateBlobContainers()
                    : Task.Run(() => { }),
                migrateTables
                    ? MigrateTableStorage()
                    : Task.Run(() => { }),
            };

            Task.WaitAll(tasks);
            return "done";
        }

        private Task MigrateTableStorage()
        {
            return Task.Run(() =>
            {
                CopyTableStorageFromSource();
                return "done";
            });
        }

        private void CopyTableStorageFromSource()
        {
            CloudTableClient source = sourceAccount.CreateCloudTableClient();

            var cloudTables = source.ListTables()
                .OrderBy(c => c.Name)
                .ToList();
            
            var tablesToMigrate = CloudConfigurationManager
                .GetSetting("TablesToMigrate")
                .Split(new[] {"|"}, StringSplitOptions.RemoveEmptyEntries);

            foreach (var table in cloudTables)
            {
                if (tablesToMigrate.Contains("*") || tablesToMigrate.Contains(table.Name))
                   CopyTables(table);
            }
        }

        private void CopyTables(CloudTable table)
        {
            var target = targetAccount.CreateCloudTableClient();

            var targetTable = target.GetTableReference(table.Name);

            targetTable.CreateIfNotExists();

            targetTable.SetPermissions(table.GetPermissions());

            Console.WriteLine("Created Table Storage :" + table.Name);

            var omit = CloudConfigurationManager
                .GetSetting("TablesToCreateButNotMigrate")
                .Split(new[] { "|" }, StringSplitOptions.RemoveEmptyEntries);

            if (!omit.Contains(table.Name))
                CopyData(table);
        }

        private readonly List<ICancellableAsyncResult> queries
            = new List<ICancellableAsyncResult>();

        private readonly Dictionary<string, long> retrieved
            = new Dictionary<string, long>();

        private readonly TableQuery<DynamicTableEntity> query
            = new TableQuery<DynamicTableEntity>();

        private void CopyData(CloudTable table)
        {
            ExecuteQuerySegment(table, null);
        }

        private void ExecuteQuerySegment(CloudTable table,
            TableContinuationToken token)
        {
            var reqOptions = new TableRequestOptions();

            var ctx = new OperationContext { ClientRequestID = "StorageMigrator" };

            do
            {
                System.Threading.ManualResetEvent evt = new System.Threading.ManualResetEvent(false);
                var result = table.BeginExecuteQuerySegmented(query, token, reqOptions, ctx, (o) =>
                {
                    var cloudTable = o.AsyncState as CloudTable;
                    if (cloudTable == null) return;

                    var response = cloudTable.EndExecuteQuerySegmented<DynamicTableEntity>(o);
                    token = response.ContinuationToken;
                    var retrieved = response.Count();
                    if (retrieved > 0 || token != null)
                    {
                        //Task.Run(() => WriteToTarget(cloudTable, response));
                        WriteToTarget(cloudTable, response);
                    }

                    var recordsRetrieved = retrieved;

                    UpdateCount(cloudTable, recordsRetrieved);

                    Console.WriteLine("Table " +
                                      cloudTable.Name +
                                      " |> Records = " +
                                      recordsRetrieved +
                                      " | Total Records = " +
                                      this.retrieved[cloudTable.Name]);
                    evt.Set();

                }, table);

                evt.WaitOne();

            } while (token != null);

            Console.WriteLine("Done with " + table.Name);
        }

        private void UpdateCount(CloudTable cloudTable, int recordsRetrieved)
        {
            if (!retrieved.ContainsKey(cloudTable.Name))
                retrieved.Add(cloudTable.Name, recordsRetrieved);
            else
                retrieved[cloudTable.Name] += recordsRetrieved;
        }

        private static void WriteToTarget(CloudTable cloudTable,
            IEnumerable<DynamicTableEntity> response)
        {
            var writer = new TableStorageWriter(cloudTable.Name, "DataConnectionTarget");
            foreach (var entity in response)
            {
                writer.InsertOrReplace(entity);
            }
            writer.Execute();
        }

        private void _MigrateBlobContainers()
        {
            CopyBlobContainersFromSource();
        }

        public Task<string> MigrateBlobContainers()
        {
            return Task.Run(() =>
            {
                CopyBlobContainersFromSource();
                return "done";
            });
        }

        private void CopyBlobContainersFromSource()
        {
            var source = sourceAccount.CreateCloudBlobClient();

            var cloudBlobContainers = source.ListContainers()
                .OrderBy(c => c.Name)
                .ToList();

            foreach (var cloudBlobContainer in cloudBlobContainers)
                CopyBlobContainer(cloudBlobContainer);
        }

        private void CopyBlobContainer(CloudBlobContainer sourceContainer)
        {
            var targetContainer = MakeContainer(sourceContainer);

            var targetBlobs = targetContainer.ListBlobs(null,
                true,
                BlobListingDetails.All)
                .Select(b => (ICloudBlob)b)
                .ToList();

            Debug.WriteLine(sourceContainer.Name + " Created");

            Debug.WriteLine(sourceContainer.Name + " List all blobs");

            var sourceBlobs = sourceContainer
                .ListBlobs(null,
                    true,
                    BlobListingDetails.All)
                .Select(b => (ICloudBlob)b)
                .ToList();

            var missingBlobTask = Task.Run(() =>
            {
                AddMissingBlobs(sourceContainer,
                    sourceBlobs,
                    targetBlobs,
                    targetContainer);
            });

            var updateBlobs = Task.Run(() => UpdateBlobs(sourceContainer,
                sourceBlobs,
                targetBlobs,
                targetContainer));

            Task.WaitAll(new[] { missingBlobTask, updateBlobs });

        }

        private void UpdateBlobs(CloudBlobContainer sourceContainer,
            IEnumerable<ICloudBlob> sourceBlobs,
            IEnumerable<ICloudBlob> targetBlobs,
            CloudBlobContainer targetContainer)
        {
            var updatedBlobs = sourceBlobs
                .AsParallel()
                .Select(sb =>
                {
                    var tb = targetBlobs.FirstOrDefault(b => b.Name == sb.Name);
                    if (tb == null)
                        return new
                        {
                            Source = sb,
                            Target = sb,
                        };

                    if (tb.Properties.LastModified < sb.Properties.LastModified)
                        return new
                        {
                            Source = sb,
                            Target = tb,
                        };

                    return new
                    {
                        Source = sb,
                        Target = sb,
                    };
                })
                .Where(b => b.Source != b.Target)
                .ToList();

            Console.WriteLine(targetContainer.Name + " |> " +
                              "Updating :" +
                              updatedBlobs.Count +
                              " blobs");

            Debug.WriteLine(sourceContainer.Name + " Start update all blobs");

            Parallel.ForEach(updatedBlobs, blob =>
            {
                TryCopyBlobToTargetContainer(blob.Source,
                    targetContainer,
                    sourceContainer);
            });

            Debug.WriteLine(sourceContainer.Name + " End update all blobs");
        }

        private void AddMissingBlobs(CloudBlobContainer sourceContainer,
            IEnumerable<ICloudBlob> sourceBlobs,
            IEnumerable<ICloudBlob> targetBlobs,
            CloudBlobContainer targetContainer)
        {
            var missingBlobs = sourceBlobs.AsParallel()
                .Where(b => NotExists(targetBlobs, b))
                .ToList();

            Console.WriteLine(targetContainer.Name +
                              " |> " +
                              "Adding missing :" +
                              missingBlobs.Count +
                              " blobs");

            Debug.WriteLine(sourceContainer.Name + " Start copy missing blobs");

            Parallel.ForEach(missingBlobs, blob =>
            {
                TryCopyBlobToTargetContainer(blob,
                    targetContainer,
                    sourceContainer);
            });

            Debug.WriteLine(sourceContainer.Name + " End copy missing blobs");
        }

        private static bool NotExists(IEnumerable<ICloudBlob> targetBlobs,
            ICloudBlob b)
        {
            return targetBlobs.All(tb => tb.Name != b.Name);
        }

        private CloudBlobContainer MakeContainer(CloudBlobContainer sourceContainer)
        {
            var target = targetAccount.CreateCloudBlobClient();
            var targetContainer = target.GetContainerReference(sourceContainer.Name);

            Debug.WriteLine(sourceContainer.Name + " Started");

            targetContainer.CreateIfNotExists();

            var blobContainerPermissions = sourceContainer.GetPermissions();

            if (blobContainerPermissions != null)
                targetContainer.SetPermissions(blobContainerPermissions);

            Debug.WriteLine(sourceContainer.Name + " Set Permissions");

            foreach (var meta in sourceContainer.Metadata)
                targetContainer.Metadata.Add(meta);

            targetContainer.SetMetadata();

            Debug.WriteLine(sourceContainer.Name + " Set Metadata");

            return targetContainer;
        }

        private void TryCopyBlobToTargetContainer(ICloudBlob item,
            CloudBlobContainer targetContainer,
            CloudBlobContainer sourceContainer)
        {
            try
            {
                var blob = (CloudBlockBlob)item;
                var blobRef = targetContainer.GetBlockBlobReference(blob.Name);

                var source = new Uri(GetShareAccessUri(blob.Name,
                    360,
                    sourceContainer));
                var result = blobRef.StartCopyFromBlob(source);
                Debug.WriteLine(blob.Properties.LastModified.ToString() +
                                " |>" +
                                blob.Name +
                                " :" +
                                result);
            }
            catch (StorageException ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }

        private string GetShareAccessUri(string blobname,
            int validityPeriodInMinutes,
            CloudBlobContainer container)
        {
            var toDateTime = DateTime.Now.AddMinutes(validityPeriodInMinutes);

            var policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };

            var blob = container.GetBlockBlobReference(blobname);
            var sas = blob.GetSharedAccessSignature(policy);
            return blob.Uri.AbsoluteUri + sas;
        }
    }


}