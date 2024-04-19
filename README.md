# CosmosDbClone

CosmosDbClone is a .NET tool designed to simplify the process of cloning Azure Cosmos DB databases.

With this tool, you can replicate your databases, collections, and documents without the hassle of manual copying or complex scripts.

## Configuration

CosmosDbClone is configured through the `appsettings.json` file, allowing you to specify the source and target database names, as well as the connection string to your source Cosmos DB:

```json
{
  "SourceDbName": "",
  "TargetDbName": "",
  "ConnectionStrings": {
    "Source": ""
  }
}
```

Be sure to fill in the **SourceDbName** and **TargetDbName** with the names of your source and target databases, respectively.
The **Source** connection string should be updated with the connection details to your source Cosmos DB account.

## Features

- **Single Account Support**: currently, CosmosDbClone supports cloning within a single Cosmos DB account.
- **Database Creation**: if the target database specified does not exist, it will be automatically created.
- **Containers Creation**: similarly, if the target containers do not exist in the target database, they will be created during the cloning process.

## Todo

- **Configurable Parallelism**: allow configuration for the number of containers that can be cloned in parallel (currently set to 3 containers).
- **Configurable Documents Batch Size**: configure the batch size for document cloning (currently set at 500 documents per batch).
- **Multi-Account Support**: cloning across multiple Cosmos DB accounts.

## License

CosmosDbClone is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
