using System.Text.Json.Serialization;

namespace MatchQueueService.Contracts;

public sealed record TriggerMatchRequest(
    [property: JsonPropertyName("started_at")] DateTime StartedAt,
    [property: JsonPropertyName("finished_at")] DateTime FinishedAt);

