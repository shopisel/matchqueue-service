using System.Text.Json;
using System.Linq;
using MatchQueueService.Contracts;
using MatchQueueService.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MatchQueueService.Worker;

public static class GitHubDispatchWorker
{
    public static async Task<int> RunAsync(IServiceProvider services, string[] args, CancellationToken ct)
    {
        using var scope = services.CreateScope();
        var scopedServices = scope.ServiceProvider;

        var loggerFactory = scopedServices.GetRequiredService<ILoggerFactory>();
        var logger = loggerFactory.CreateLogger("MatchQueueService.Worker.GitHubDispatch");

        var eventPath = Environment.GetEnvironmentVariable("GITHUB_EVENT_PATH");
        if (string.IsNullOrWhiteSpace(eventPath) || !File.Exists(eventPath))
        {
            logger.LogError(
                "GITHUB_EVENT_PATH is missing or file does not exist. path={EventPath}",
                eventPath ?? "<null>");
            return 2;
        }

        logger.LogInformation("Reading GitHub event payload. path={EventPath}", eventPath);

        JsonDocument? doc = null;
        try
        {
            await using var stream = File.OpenRead(eventPath);
            doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to read/parse GitHub event payload.");
            return 2;
        }

        using (doc)
        {
            var eventName = Environment.GetEnvironmentVariable("GITHUB_EVENT_NAME");

            JsonElement? payloadObject = null;

            if (doc.RootElement.TryGetProperty("client_payload", out var clientPayload)
                && clientPayload.ValueKind is JsonValueKind.Object)
            {
                payloadObject = clientPayload;
            }
            else if (doc.RootElement.TryGetProperty("inputs", out var inputs)
                && inputs.ValueKind is JsonValueKind.Object)
            {
                payloadObject = inputs;
            }

            var matchQueueService = scopedServices.GetRequiredService<IMatchQueueService>();

            if (doc.RootElement.TryGetProperty("workflow_run", out var workflowRun)
                && workflowRun.ValueKind is JsonValueKind.Object)
            {
                var workflowRunId = ReadString(workflowRun, "id");
                if (!string.IsNullOrWhiteSpace(workflowRunId))
                {
                    var result = await matchQueueService.ProcessRunAsync(workflowRunId!, ct);
                    logger.LogInformation(
                        "Worker completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                        result.RunId,
                        result.ProductsProcessed,
                        result.ProductsCreated,
                        result.PricesUpserted,
                        result.NotificationsEnqueued);
                    return 0;
                }
            }

            if (payloadObject is null)
            {
                var keys = string.Join(
                    ",",
                    doc.RootElement.EnumerateObject().Select(p => p.Name).Take(25));

                logger.LogError(
                    "Unsupported GitHub event payload (missing client_payload/inputs/workflow_run.id). event_name={EventName} keys={Keys}",
                    eventName ?? "<null>",
                    keys);
                return 2;
            }

            var payload = payloadObject.Value;

            var anyFailed = ReadBool(payload, "any_failed") ?? false;
            if (anyFailed)
            {
                logger.LogWarning("Payload indicates upstream failures (any_failed=true). Continuing anyway.");
            }

            var scrapers = ReadObject(payload, "scrapers");
            var scrapersOutputs = ReadObject(scrapers, "outputs");

            var runId =
                ReadString(scrapersOutputs, "run_id")
                ?? ReadString(scrapersOutputs, "runId")
                ?? ReadString(payload, "run_id");

            var startedAt =
                ReadDateTime(scrapersOutputs, "started_at")
                ?? ReadDateTime(scrapersOutputs, "startedAt")
                ?? ReadDateTime(payload, "started_at")
                ?? ReadDateTime(payload, "startedAt");

            var finishedAt =
                ReadDateTime(scrapersOutputs, "finished_at")
                ?? ReadDateTime(scrapersOutputs, "finishedAt")
                ?? ReadDateTime(payload, "finished_at")
                ?? ReadDateTime(payload, "finishedAt");

            if (!string.IsNullOrWhiteSpace(runId))
            {
                var result = await matchQueueService.ProcessRunAsync(runId!, ct);
                if (result.ProductsProcessed > 0)
                {
                    logger.LogInformation(
                        "Worker completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                        result.RunId,
                        result.ProductsProcessed,
                        result.ProductsCreated,
                        result.PricesUpserted,
                        result.NotificationsEnqueued);
                    return 0;
                }

                if (startedAt is null || finishedAt is null)
                {
                    logger.LogWarning(
                        "run_id provided but no products found and no started_at/finished_at provided; stopping. run_id={RunId}",
                        runId);
                    return 0;
                }

                logger.LogWarning(
                    "run_id provided but no products found; falling back to timestamp-based trigger. run_id={RunId} started_at={StartedAt:o} finished_at={FinishedAt:o}",
                    runId,
                    startedAt.Value,
                    finishedAt.Value);
            }

            if (startedAt is null || finishedAt is null)
            {
                var allowLatestFallback = ReadBool(payload, "fallback_latest_run")
                    ?? ReadEnvBool("MATCHQUEUE_FALLBACK_LATEST_RUN")
                    ?? false;

                if (!allowLatestFallback)
                {
                    logger.LogError(
                        "Payload must include either 'run_id' or both 'started_at' and 'finished_at'.");
                    return 2;
                }

                var maxAgeMinutes = ReadEnvInt("MATCHQUEUE_LATEST_RUN_MAX_AGE_MINUTES") ?? 60;

                var mongoDatabase = scopedServices.GetService<IMongoDatabase>();
                if (mongoDatabase is null)
                {
                    logger.LogError(
                        "MATCHQUEUE_FALLBACK_LATEST_RUN enabled but IMongoDatabase is not registered.");
                    return 2;
                }

                var latestRun = await TryResolveLatestRunAsync(mongoDatabase, maxAgeMinutes, ct);
                if (latestRun is null)
                {
                    logger.LogError(
                        "MATCHQUEUE_FALLBACK_LATEST_RUN enabled but no recent scrape_runs found within max_age_minutes={MaxAgeMinutes}.",
                        maxAgeMinutes);
                    return 2;
                }

                logger.LogWarning(
                    "No run_id/timestamps provided; falling back to latest scrape run. run_id={RunId} started_at={StartedAt:o} finished_at={FinishedAt:o}",
                    latestRun.RunId,
                    latestRun.StartedAtUtc,
                    latestRun.FinishedAtUtc);

                var result = await matchQueueService.ProcessRunAsync(latestRun.RunId, ct);
                logger.LogInformation(
                    "Worker completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                    result.RunId,
                    result.ProductsProcessed,
                    result.ProductsCreated,
                    result.PricesUpserted,
                    result.NotificationsEnqueued);
                return 0;
            }

            var request = new TriggerMatchRequest(startedAt.Value, finishedAt.Value);
            var response = await matchQueueService.ProcessAsync(request, ct);

            logger.LogInformation(
                "Worker completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                response.RunId,
                response.ProductsProcessed,
                response.ProductsCreated,
                response.PricesUpserted,
                response.NotificationsEnqueued);
            return 0;
        }
    }

    private static JsonElement? ReadObject(JsonElement? obj, string propertyName)
    {
        if (obj is null)
        {
            return null;
        }

        var value = obj.Value;
        if (value.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        if (!value.TryGetProperty(propertyName, out var property))
        {
            return null;
        }

        return property.ValueKind == JsonValueKind.Object ? property : null;
    }

    private static bool? ReadEnvBool(string key)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return bool.TryParse(raw, out var parsed) ? parsed : null;
    }

    private static int? ReadEnvInt(string key)
    {
        var raw = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return int.TryParse(raw, out var parsed) ? parsed : null;
    }

    private static string? ReadString(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.GetRawText(),
            _ => null
        };
    }

    private static string? ReadString(JsonElement? obj, string propertyName)
    {
        if (obj is null)
        {
            return null;
        }

        return ReadString(obj.Value, propertyName);
    }

    private static bool? ReadBool(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        return value.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String when bool.TryParse(value.GetString(), out var parsed) => parsed,
            _ => null
        };
    }

    private static DateTime? ReadDateTime(JsonElement obj, string propertyName)
    {
        if (!obj.TryGetProperty(propertyName, out var value))
        {
            return null;
        }

        if (value.ValueKind == JsonValueKind.String && DateTime.TryParse(value.GetString(), out var parsed))
        {
            return parsed;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var unixSeconds))
        {
            return DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
        }

        return null;
    }

    private static DateTime? ReadDateTime(JsonElement? obj, string propertyName)
    {
        if (obj is null)
        {
            return null;
        }

        return ReadDateTime(obj.Value, propertyName);
    }

    private static async Task<LatestRun?> TryResolveLatestRunAsync(
        IMongoDatabase mongoDatabase,
        int maxAgeMinutes,
        CancellationToken ct)
    {
        var runs = mongoDatabase.GetCollection<BsonDocument>("scrape_runs");

        var now = DateTime.UtcNow;
        var minFinishedAt = now.AddMinutes(-Math.Max(1, maxAgeMinutes));

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Ne("finished_at", BsonNull.Value),
            Builders<BsonDocument>.Filter.Gte("finished_at", minFinishedAt));

        var projection = Builders<BsonDocument>.Projection
            .Include("_id")
            .Include("started_at")
            .Include("finished_at");

        var doc = await runs
            .Find(filter)
            .Sort(Builders<BsonDocument>.Sort.Descending("finished_at"))
            .Project(projection)
            .Limit(1)
            .FirstOrDefaultAsync(ct);

        if (doc is null)
        {
            return null;
        }

        var runId = doc.GetValue("_id", BsonNull.Value).ToString();
        if (string.IsNullOrWhiteSpace(runId) || runId == "BsonNull")
        {
            return null;
        }

        var startedAtUtc = TryGetUtc(doc, "started_at") ?? now;
        var finishedAtUtc = TryGetUtc(doc, "finished_at") ?? now;

        return new LatestRun(runId, startedAtUtc, finishedAtUtc);
    }

    private static DateTime? TryGetUtc(BsonDocument doc, string field)
    {
        if (!doc.TryGetValue(field, out var value) || value.IsBsonNull)
        {
            return null;
        }

        try
        {
            return value.ToUniversalTime();
        }
        catch
        {
            return null;
        }
    }

    private sealed record LatestRun(string RunId, DateTime StartedAtUtc, DateTime FinishedAtUtc);
}
