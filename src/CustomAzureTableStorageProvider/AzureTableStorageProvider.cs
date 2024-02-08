using Azure;
using Azure.Data.Tables;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using System.Diagnostics;

namespace CustomAzureTableStorageProvider;

/// <summary>
/// This storage provider assumes they grain key is formed like that: YYYY-MM-dd_someKey
/// </summary>
public class AzureTableStorageProvider : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private TableClient? _tableClient;
    private readonly TableServiceClient _tableServiceClient;

    public AzureTableStorageProvider(TableServiceClient tableServiceClient)
    {
        _tableServiceClient = tableServiceClient;
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        Debug.Assert(_tableClient != null);
        var pk = ExtractPk(grainId);
        var rk = ExtractRk(grainId, stateName);

        await _tableClient.DeleteEntityAsync(pk, rk, new ETag(grainState.ETag));
        grainState.State = Activator.CreateInstance<T>();
        grainState.ETag = default;
        grainState.RecordExists = false;
    }

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        Debug.Assert(_tableClient != null);
        var pk = ExtractPk(grainId);
        var rk = ExtractRk(grainId, stateName);

        var response = await _tableClient.GetEntityIfExistsAsync<TableEntity>(pk, rk);
        if (!response.HasValue)
        {
            grainState.State = Activator.CreateInstance<T>();
            grainState.ETag = default;
            grainState.RecordExists = false;
            return;
        }

        var data = new BinaryData(response.Value["data"].ToString());
        grainState.State = data.ToObjectFromJson<T>();
        grainState.ETag = response.Value.ETag.ToString();
        grainState.RecordExists = true;
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        Debug.Assert(_tableClient != null);
        var pk = ExtractPk(grainId);
        var rk = ExtractRk(grainId, stateName);

        var data = new BinaryData(grainState.State).ToString();
        var entity = new TableEntity(pk, rk);
        if (!string.IsNullOrEmpty(grainState.ETag))
        {
            entity.ETag = new ETag(grainState.ETag);
        }
        entity["data"] = data;
        var result = await _tableClient.UpsertEntityAsync(entity, mode: TableUpdateMode.Replace);
        grainState.RecordExists = true;
        grainState.ETag = result.Headers.ETag.ToString();
    }

    public void Participate(ISiloLifecycle observer)
    {
        // Create the table on silo startup if it doesn't exist
        observer.Subscribe(nameof(AzureTableStorageProvider), ServiceLifecycleStage.ApplicationServices, onStart);
        async Task onStart(CancellationToken ct)
        {
            _tableClient = _tableServiceClient.GetTableClient("GrainState");
            await _tableClient.CreateIfNotExistsAsync(ct);
        }
    }

    private string ExtractPk(GrainId grainId)
    {
        var key = grainId.Key.ToString();
        Debug.Assert(key != null);
        return key.Substring(0, 10);
    }

    private string ExtractRk(GrainId grainId, string stateName)
    {
        var key = grainId.Key.ToString();
        Debug.Assert(key != null);
        return $"{key.Substring(11)}-{stateName}";
    }
}
