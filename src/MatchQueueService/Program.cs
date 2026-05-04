using MatchQueueService.Data;
using MatchQueueService.Endpoints;
using MatchQueueService.Services;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using MongoDB.Driver;
using System.Security.Authentication;
using MatchQueueService.Worker;
using System.Linq;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();

var connectionString = builder.Configuration.GetConnectionString("MatchQueueService");
if (string.IsNullOrWhiteSpace(connectionString))
{
    throw new InvalidOperationException(
        "Connection string 'ConnectionStrings:MatchQueueService' is required.");
}

builder.Services.AddDbContext<MatchQueueDbContext>(options =>
{
    options
        .UseNpgsql(connectionString)
        .ConfigureWarnings(warnings => warnings.Ignore(RelationalEventId.PendingModelChangesWarning));
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

builder.Services.AddSingleton<IMongoClient>(_ =>
{
    var settings = MongoClientSettings.FromConnectionString(mongoConnectionString);
    settings.SslSettings = new SslSettings
    {
        EnabledSslProtocols = SslProtocols.Tls12
    };
    return new MongoClient(settings);
});
builder.Services.AddSingleton<IMongoDatabase>(sp =>
    sp.GetRequiredService<IMongoClient>().GetDatabase(mongoDatabaseName));

builder.Services.AddScoped<IMatchQueueService, MatchQueueService.Services.MatchQueueService>();

var app = builder.Build();

var mode = GetArgValue(args, "--mode");
var isWorker = string.Equals(mode, "worker", StringComparison.OrdinalIgnoreCase)
    || args.Any(arg => string.Equals(arg, "worker", StringComparison.OrdinalIgnoreCase));

app.UseExceptionHandler(exceptionApp =>
{
    exceptionApp.Run(async context =>
    {
        var logger = context.RequestServices
            .GetRequiredService<ILoggerFactory>()
            .CreateLogger("MatchQueueService.UnhandledException");

        var exception = context.Features.Get<IExceptionHandlerFeature>()?.Error;
        if (exception is not null)
        {
            logger.LogError(exception, "Unhandled exception while processing request.");
        }

        context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        context.Response.ContentType = "application/problem+json";

        var problem = new ProblemDetails
        {
            Status = StatusCodes.Status500InternalServerError,
            Title = "Internal Server Error",
            Detail = "An unexpected error occurred.",
            Instance = context.Request.Path
        };
        problem.Extensions["traceId"] = context.TraceIdentifier;

        await context.Response.WriteAsJsonAsync(problem);
    });
});

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

await InitializeDatabaseAsync(app);

if (isWorker)
{
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("MatchQueueService.Worker");
    logger.LogInformation("Starting GitHub repository_dispatch worker mode.");

    var exitCode = await GitHubDispatchWorker.RunAsync(app.Services, args, app.Lifetime.ApplicationStopping);
    Environment.ExitCode = exitCode;
    return;
}

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

static string? GetArgValue(string[] args, string name)
{
    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        if (arg.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
        {
            return arg[(name.Length + 1)..];
        }

        if (string.Equals(arg, name, StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        {
            return args[i + 1];
        }
    }

    return null;
}

public partial class Program;
