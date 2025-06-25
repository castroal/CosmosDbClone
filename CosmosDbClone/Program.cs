using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

namespace CosmosDbClone
{
    internal class Program
    {
        readonly static SemaphoreSlim maxParallelism = new(3, 3);

        static async Task Main(string[] _)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false);
            IConfigurationRoot configuration = builder.Build();

            var sourceConnectionString = configuration.GetConnectionString("Source");
            var targetConnectionString = configuration.GetConnectionString("Target") ?? sourceConnectionString;
            var sourceDatabaseName = configuration.GetValue<string>("SourceDbName");
            var targetDatabaseName = configuration.GetValue<string>("TargetDbName");

            using var sourceClient = new CosmosClient(sourceConnectionString);
            using var targetClient = targetConnectionString == sourceConnectionString 
                ? sourceClient 
                : new CosmosClient(targetConnectionString);

            var sourceDatabase = sourceClient.GetDatabase(sourceDatabaseName);
            var targetDatabase = (await targetClient.CreateDatabaseIfNotExistsAsync(targetDatabaseName)).Database;

            AnsiConsole.MarkupLine($"Copying [green]{sourceDatabaseName} {sourceClient.Endpoint.Host}[/] to [red]{targetDatabaseName} {targetClient.Endpoint.Host}[/] ...");

            var progress = AnsiConsole.Progress();
            progress.RefreshRate = TimeSpan.FromSeconds(0.5);

            await progress
                .AutoClear(false)
                .HideCompleted(false)
                .StartAsync(async ctx =>
                {
                    var copyTasks = new List<Task>();
                    var sourceContainersIterator = sourceDatabase.GetContainerQueryIterator<ContainerProperties>();
                    do
                    {
                        var containers = await sourceContainersIterator.ReadNextAsync();
                        foreach (var sourceContainerProperties in containers)
                        {
                            var progressTask = ctx.AddTask($"{sourceContainerProperties.Id}");
                            copyTasks.Add(CloneContainerAsync(sourceDatabase, targetDatabase, sourceContainerProperties, progressTask));
                        }
                    }
                    while (sourceContainersIterator.HasMoreResults);

                    await Task.WhenAll(copyTasks);
                });

            AnsiConsole.MarkupLine("[green]Done![/]");
        }

        private static async Task CloneContainerAsync(Database sourceDatabase, Database targetDatabase, ContainerProperties sContainerProperties, ProgressTask progressTask)
        {
            await maxParallelism.WaitAsync();
            try
            {
                progressTask.StartTask();
                var sourceContainer = sourceDatabase.GetContainer(sContainerProperties.Id);
                var targetContainerProperties = new ContainerProperties
                {
                    Id = sContainerProperties.Id,
                    PartitionKeyPath = sContainerProperties.PartitionKeyPath,
                    UniqueKeyPolicy = sContainerProperties.UniqueKeyPolicy,
                };
                var targetContainer = (await targetDatabase.CreateContainerIfNotExistsAsync(targetContainerProperties)).Container;

                progressTask.MaxValue = (await sourceContainer.GetItemQueryIterator<int>("SELECT VALUE COUNT(1) FROM c").ReadNextAsync()).Resource.First();

                var sourceIterator = sourceContainer.GetItemQueryIterator<dynamic>(requestOptions: new QueryRequestOptions { MaxItemCount = 500 });
                do
                {
                    var sourceItems = await sourceIterator.ReadNextAsync();
                    await Parallel.ForEachAsync(sourceItems, async (sourceItem, _) => await targetContainer.CreateItemAsync(sourceItem));
                    progressTask.Increment(sourceItems.Count);
                }
                while (sourceIterator.HasMoreResults);
                progressTask.StopTask();
            }
            finally
            {
                maxParallelism.Release();
            }
        }
    }
}
