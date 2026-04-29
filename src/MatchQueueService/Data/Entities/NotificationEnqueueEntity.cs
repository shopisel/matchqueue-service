namespace MatchQueueService.Data.Entities;

public sealed class NotificationEnqueueEntity
{
    public string Id { get; set; } = string.Empty;
    public string AccountId { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public bool Checked { get; set; }
}

