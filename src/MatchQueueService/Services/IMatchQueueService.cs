using MatchQueueService.Contracts;

namespace MatchQueueService.Services;

public interface IMatchQueueService
{
    Task<TriggerMatchResponse> ProcessAsync(TriggerMatchRequest request, CancellationToken ct);
}

