using Microsoft.Extensions.Hosting;
using Orleans.Providers;
using Orleans.Runtime.Hosting;
using CustomAzureTableStorageProvider;
using Microsoft.Extensions.DependencyInjection;
using Azure.Data.Tables;

var builder = Host
    .CreateDefaultBuilder(args)
    .ConfigureServices(services => services.AddOrleans(ConfigureOrleans));

var host = builder.Build();

await host.StartAsync();

var client = host.Services.GetRequiredService<IClusterClient>();
var grain = client.GetGrain<ICounterGrain>("2024-02-07_SomeCounter");

await grain.Increment();
for (int i = 1; i < 10; i++)
{
    await Task.Delay(1000);
    await grain.Increment();
}

await host.StopAsync(); 
    
    
void ConfigureOrleans(ISiloBuilder siloBuilder)
{
    siloBuilder.UseLocalhostClustering();

    siloBuilder.Services
        .AddSingleton(new TableServiceClient("UseDevelopmentStorage=true"))
        .AddGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, (sp, name) => ActivatorUtilities.CreateInstance<AzureTableStorageProvider>(sp));
}

public interface ICounterGrain : IGrainWithStringKey
{
    Task<int> Increment();
}

public class CounterGrainState
{
    public int Value { get; set; }
}

public class CounterGrain : Grain<CounterGrainState>, ICounterGrain
{
    public async Task<int> Increment()
    {
        State.Value++;
        await WriteStateAsync();
        return State.Value;
    }
}