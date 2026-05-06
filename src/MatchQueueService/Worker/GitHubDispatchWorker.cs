using System.Text.Json;
using MatchQueueService.Contracts;
using MatchQueueService.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
            if (!doc.RootElement.TryGetProperty("client_payload", out var clientPayload)
                || clientPayload.ValueKind is not JsonValueKind.Object)
            {
                logger.LogError("Missing github.event.client_payload on repository_dispatch payload.");
                return 2;
            }

            var anyFailed = ReadBool(clientPayload, "any_failed") ?? false;
            if (anyFailed)
            {
                logger.LogWarning("Payload indicates upstream failures (any_failed=true). Continuing anyway.");
            }

            var matchQueueService = scopedServices.GetRequiredService<IMatchQueueService>();

            var runId = ReadString(clientPayload, "run_id");
            if (!string.IsNullOrWhiteSpace(runId))
            {
                var result = await matchQueueService.ProcessRunAsync(runId!, ct);
                logger.LogInformation(
                    "Worker completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                    result.RunId,
                    result.ProductsProcessed,
                    result.ProductsCreated,
                    result.PricesUpserted,
                    result.NotificationsEnqueued);
                return 0;
            }

            var startedAt = ReadDateTime(clientPayload, "started_at") ?? ReadDateTime(clientPayload, "startedAt");
            var finishedAt = ReadDateTime(clientPayload, "finished_at") ?? ReadDateTime(clientPayload, "finishedAt");

            if (startedAt is null || finishedAt is null)
            {
                logger.LogError(
                    "Payload must include either 'run_id' or both 'started_at' and 'finished_at'.");
                return 2;
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
}
