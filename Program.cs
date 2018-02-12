using System;
using System.Threading.Tasks;
using Microsoft.Azure.Devices;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace IoTHubDeviceIdentitySync
{
    class Program
    {
        const string originIotHubConnectionString = "HostName=<IoT Hub Name>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<Key>";
        const string destinationIotHubConnectionString = "HostName=<IoT Hub Name>.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=<Key>";
        const string storageAccountAccessKey = "DefaultEndpointsProtocol=https;AccountName=<Accout name>;AccountKey=<Key>;EndpointSuffix=core.windows.net";

        static async Task Main(string[] args)
        {
            var originRegistryManager = RegistryManager.CreateFromConnectionString(originIotHubConnectionString);
            var destinationRegistryManager = RegistryManager.CreateFromConnectionString(destinationIotHubConnectionString);

            Console.WriteLine("List of all devices in the Origin IoT Hub (Query max returns 1000 items):");
            await ListDevices(originRegistryManager);

            var container = await GetContainerAndCreateIfNotExists("deviceidentities");
            var containerUri = GetContainerSasUri(container);

            Console.WriteLine(@"Location to store the exported device identities (ContainerURI with SAS Token): " + containerUri);
            
            // var device = manager.GetDeviceAsync("t1").Result;

            Console.WriteLine("Exporting from Origin..");
            await ExportDeviceIdentities(originRegistryManager, containerUri);

            Console.WriteLine("Importing to Destination..");
            await ImportDeviceIdentities(destinationRegistryManager, containerUri);


            Console.WriteLine("Done!");
            Console.ReadLine();
        }

        static async Task ExportDeviceIdentities(RegistryManager registryManager, string containerUri)
        {
            var job = await registryManager.ExportDevicesAsync(containerUri, false);
            
            await WaitForJobToComplete(registryManager, job);
        }

        static async Task ImportDeviceIdentities(RegistryManager registryManager, string containerUri)
        {
            var job = await registryManager.ImportDevicesAsync(containerUri, containerUri);
            
            await WaitForJobToComplete(registryManager, job);
        }

        static async Task WaitForJobToComplete(RegistryManager registryManager, JobProperties job)
        {
            while (true)
            {
                job = await registryManager.GetJobAsync(job.JobId);
                Console.WriteLine("\t Job " + job.Status);

                if (job.Status == JobStatus.Completed
                        || job.Status == JobStatus.Failed
                        || job.Status == JobStatus.Cancelled)
                {
                    break;
                }
    
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }

        static async Task ListDevices(RegistryManager registryManager)
        {
            var query = registryManager.CreateQuery("select * from devices");

            while (query.HasMoreResults)
            {
                var page = await query.GetNextAsTwinAsync();
                foreach (var twin in page)
                {
                    Console.WriteLine(twin.DeviceId);
                }
            }
        }

        static async Task<CloudBlobContainer> GetContainerAndCreateIfNotExists(string containerName)
        {
            var account = CloudStorageAccount.Parse(storageAccountAccessKey);
            var client = account.CreateCloudBlobClient();
    
            var container = client.GetContainerReference(containerName);

            await container.CreateIfNotExistsAsync();    

            return container;
        }

        static string GetContainerSasUri(CloudBlobContainer container)
        {
            // Set the expiry time and permissions for the container.
            // In this case no start time is specified, so the
            // shared access signature becomes valid immediately.
            var sasConstraints = new SharedAccessBlobPolicy();
            sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(1);
            sasConstraints.Permissions = 
                SharedAccessBlobPermissions.Write | 
                SharedAccessBlobPermissions.Read | 
                SharedAccessBlobPermissions.Delete;

            // Generate the shared access signature on the container,
            // setting the constraints directly on the signature.
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Return the URI string for the container,
            // including the SAS token.
            return container.Uri + sasContainerToken;
        }
    }
}