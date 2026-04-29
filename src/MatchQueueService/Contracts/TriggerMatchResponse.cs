namespace MatchQueueService.Contracts;

public sealed record TriggerMatchResponse(
    string RunId,
    int ProductsProcessed,
    int ProductsCreated,
    int PricesUpserted,
    int NotificationsEnqueued);

