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
            var sourceDatabaseName = configuration.GetValue<string>("SourceDbName");
            var targetDatabaseName = configuration.GetValue<string>("TargetDbName");

            using var cosmosClient = new CosmosClient(sourceConnectionString);

            var sDatabase = cosmosClient.GetDatabase(sourceDatabaseName);
            var tDatabase = (await cosmosClient.CreateDatabaseIfNotExistsAsync(targetDatabaseName)).Database;

            AnsiConsole.MarkupLine($"Copying [green]{sourceDatabaseName}[/] to [red]{targetDatabaseName}[/] ...");

            var progress = AnsiConsole.Progress();
            progress.RefreshRate = TimeSpan.FromSeconds(0.5);

            await progress
                .AutoClear(false)
                .HideCompleted(false)
                .StartAsync(async ctx =>
                {
                    var copyTasks = new List<Task>();
                    var sContainersIterator = sDatabase.GetContainerQueryIterator<ContainerProperties>();
                    do
                    {
                        var containers = await sContainersIterator.ReadNextAsync();
                        foreach (var sContainerProperties in containers)
                        {
                            var progressTask = ctx.AddTask($"{sContainerProperties.Id}");
                            copyTasks.Add(CloneContainerAsync(sDatabase, tDatabase, sContainerProperties, progressTask));
                        }
                    }
                    while (sContainersIterator.HasMoreResults);

                    await Task.WhenAll(copyTasks);
                });

            AnsiConsole.MarkupLine("[green]Done![/]");
        }

        private static async Task CloneContainerAsync(Database sDatabase, Database tDatabase, ContainerProperties sContainerProperties, ProgressTask progressTask)
        {
            await maxParallelism.WaitAsync();
            try
            {
                progressTask.StartTask();
                var sContainer = sDatabase.GetContainer(sContainerProperties.Id);
                var tContainerProperties = new ContainerProperties
                {
                    Id = sContainerProperties.Id,
                    PartitionKeyPath = sContainerProperties.PartitionKeyPath,
                    UniqueKeyPolicy = sContainerProperties.UniqueKeyPolicy,
                };
                var tContainer = (await tDatabase.CreateContainerIfNotExistsAsync(tContainerProperties)).Container;

                progressTask.MaxValue = (await sContainer.GetItemQueryIterator<int>("SELECT VALUE COUNT(1) FROM c").ReadNextAsync()).Resource.First();

                var sIterator = sContainer.GetItemQueryIterator<dynamic>(requestOptions: new QueryRequestOptions { MaxItemCount = 500 });
                do
                {
                    var sItems = await sIterator.ReadNextAsync();
                    await Parallel.ForEachAsync(sItems, async (sItem, _) => await tContainer.CreateItemAsync(sItem));
                    progressTask.Increment(sItems.Count);
                }
                while (sIterator.HasMoreResults);
                progressTask.StopTask();
            }
            finally
            {
                maxParallelism.Release();
            }
        }
    }
}
