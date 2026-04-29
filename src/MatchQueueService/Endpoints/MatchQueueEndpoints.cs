using MatchQueueService.Contracts;
using MatchQueueService.Services;
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
                CancellationToken ct) =>
            {
                var result = await service.ProcessAsync(request, ct);
                return Results.Ok(result);
            })
            .WithName("TriggerMatchQueue")
            .WithSummary("Disparar o match de produtos após scraping");
    }
}

