namespace MatchQueueService.Data.Entities;

public sealed class PriceEntity
{
    public string Id { get; set; } = string.Empty;
    public string ProductId { get; set; } = string.Empty;
    public string StoreId { get; set; } = string.Empty;
    public string PriceText { get; set; } = string.Empty;
    public string? SaleText { get; set; }
    public string? QuantityText { get; set; }
    public string? UnitPriceText { get; set; }
    public DateTime? SaleDate { get; set; }
    public DateTime UpdatedAt { get; set; }
}
