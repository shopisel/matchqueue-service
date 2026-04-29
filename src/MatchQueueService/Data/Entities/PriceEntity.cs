namespace MatchQueueService.Data.Entities;

public sealed class PriceEntity
{
    public string Id { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public string StoreId { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public decimal? Sale { get; set; }
    public DateTime? SaleDate { get; set; }
    public DateTime UpdatedAt { get; set; }
}

