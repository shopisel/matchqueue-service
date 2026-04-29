using MatchQueueService.Contracts;
using MatchQueueService.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace MatchQueueService.Endpoints;

public static class MatchQueueEndpoints
{
    public static void MapMatchQueueEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app
            .MapGroup("/matchqueue")
            .WithTags("MatchQueue");

        group.MapPost("/trigger", async (
                [FromBody] TriggerMatchRequest request,
                IMatchQueueService service,
                ILoggerFactory loggerFactory,
                HttpContext httpContext,
                CancellationToken ct) =>
            {
                var logger = loggerFactory.CreateLogger("MatchQueueService.Trigger");
                using var scope = logger.BeginScope(new Dictionary<string, object?>
                {
                    ["trace_id"] = httpContext.TraceIdentifier
                });

                logger.LogInformation(
                    "Trigger request received. started_at={StartedAt:o} finished_at={FinishedAt:o}",
                    request.StartedAt,
                    request.FinishedAt);

                var result = await service.ProcessAsync(request, ct);

                logger.LogInformation(
                    "Trigger request completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
                    result.RunId,
                    result.ProductsProcessed,
                    result.ProductsCreated,
                    result.PricesUpserted,
                    result.NotificationsEnqueued);

                return Results.Ok(result);
            })
            .WithName("TriggerMatchQueue")
            .WithSummary("Disparar o match de produtos após scraping");
    }
}

