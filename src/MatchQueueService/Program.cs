using MatchQueueService.Data;
using MatchQueueService.Endpoints;
using MatchQueueService.Services;
using Microsoft.EntityFrameworkCore;
using MongoDB.Driver;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddOpenApi();

var connectionString = builder.Configuration.GetConnectionString("MatchQueueService");
if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException(
        "Connection string 'ConnectionStrings:MatchQueueService' is required.");
}

builder.Services.AddDbContext<MatchQueueDbContext>(options =>
{
    options.UseNpgsql(connectionString);
});

var mongoConnectionString = builder.Configuration["Mongo:ConnectionString"];
if (string.IsNullOrWhiteSpace(mongoConnectionString))
{
    throw new InvalidOperationException("Configuration 'Mongo:ConnectionString' is required.");
}

var mongoDatabaseName = builder.Configuration["Mongo:Database"];
if (string.IsNullOrWhiteSpace(mongoDatabaseName))
{
    mongoDatabaseName = "shopisel_scraper";
}

builder.Services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoConnectionString));
builder.Services.AddSingleton<IMongoDatabase>(sp =>
    sp.GetRequiredService<IMongoClient>().GetDatabase(mongoDatabaseName));

builder.Services.AddScoped<IMatchQueueService, MatchQueueService.Services.MatchQueueService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

await InitializeDatabaseAsync(app);

app.MapMatchQueueEndpoints();

await app.RunAsync();

static async Task InitializeDatabaseAsync(WebApplication application)
{
    using var scope = application.Services.CreateScope();
    var dbContext = scope.ServiceProvider.GetRequiredService<MatchQueueDbContext>();
    if (dbContext.Database.ProviderName?.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) == true)
    {
        await dbContext.Database.MigrateAsync();
        return;
    }

    await dbContext.Database.EnsureCreatedAsync();
}

public partial class Program;
