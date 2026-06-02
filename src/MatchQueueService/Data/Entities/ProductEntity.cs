namespace MatchQueueService.Data.Entities;

public sealed class ProductEntity
{
    public string Id { get; set; } = string.Empty;
    public string? CanonicalKey { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Brand { get; set; }
    public string Barcode { get; set; } = string.Empty;
    public string CategoryId { get; set; } = string.Empty;
    public string Image { get; set; } = string.Empty;
}
