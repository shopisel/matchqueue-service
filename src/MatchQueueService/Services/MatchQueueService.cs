using System.Globalization;
using MatchQueueService.Contracts;
using MatchQueueService.Data;
using MatchQueueService.Data.Entities;
using Microsoft.EntityFrameworkCore;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MatchQueueService.Services;

public sealed class MatchQueueService(
    MatchQueueDbContext dbContext,
    IMongoDatabase mongoDatabase,
    ILogger<MatchQueueService> logger) : IMatchQueueService
{
    private readonly IMongoCollection<BsonDocument> _runs = mongoDatabase.GetCollection<BsonDocument>("scrape_runs");
    private readonly IMongoCollection<BsonDocument> _products = mongoDatabase.GetCollection<BsonDocument>("scrape_products");

    public async Task<TriggerMatchResponse> ProcessAsync(TriggerMatchRequest request, CancellationToken ct)
    {
        var startedAt = NormalizeToUtc(request.StartedAt);
        var finishedAt = NormalizeToUtc(request.FinishedAt);

        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["started_at"] = startedAt,
            ["finished_at"] = finishedAt
        });

        logger.LogInformation("Match processing started.");

        var runId = await ResolveRunIdAsync(startedAt, finishedAt, ct);
        if (runId is null)
        {
            logger.LogWarning("No scrape run found for provided timestamps.");
            throw new InvalidOperationException(
                $"No scrape run found for started_at={startedAt:o} finished_at={finishedAt:o}.");
        }

        var scrapedProducts = await LoadProductsForRunAsync(runId, ct);
        if (scrapedProducts.Count == 0)
        {
            logger.LogWarning("No scrape products found for run_id={RunId}.", runId);
            return new TriggerMatchResponse(runId, 0, 0, 0, 0);
        }

        var productsProcessed = 0;
        var productsCreated = 0;
        var pricesUpserted = 0;
        var notificationsEnqueued = 0;

        foreach (var product in scrapedProducts)
        {
            ct.ThrowIfCancellationRequested();
            productsProcessed++;

            var name = product.ExternalName.Trim();
            var storeId = product.StoreId.Trim();
            var imageUrl = (product.ImageUrl ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(storeId))
            {
                continue;
            }

            var existing = await FindMatchAsync(name, storeId, imageUrl, ct);

            if (existing is null)
            {
                var categoryId = await ResolveCategoryIdAsync(product.CategoryId, ct);

                var createdProduct = new ProductEntity
                {
                    Id = Guid.NewGuid().ToString("N"),
                    Name = name,
                    Barcode = string.Empty,
                    CategoryId = categoryId,
                    Image = imageUrl
                };

                var createdPrice = new PriceEntity
                {
                    Id = Guid.NewGuid().ToString("N"),
                    ProductId = createdProduct.Id,
                    StoreId = storeId,
                    Price = product.Price,
                    Sale = product.Sale,
                    SaleDate = product.PromotionExpiryDate,
                    UpdatedAt = DateTime.UtcNow
                };

                dbContext.Products.Add(createdProduct);
                dbContext.Prices.Add(createdPrice);

                productsCreated++;
                pricesUpserted++;
                continue;
            }

            var previousSale = existing.Sale;

            existing.Price = product.Price;
            existing.Sale = product.Sale;
            existing.SaleDate = product.PromotionExpiryDate;
            existing.UpdatedAt = DateTime.UtcNow;

            pricesUpserted++;

            if (HasNewOrChangedSale(previousSale, existing.Sale))
            {
                var accounts = await dbContext.FavoriteProducts
                    .Where(favorite => favorite.ProductId == existing.ProductId)
                    .Select(favorite => favorite.AccountId)
                    .ToListAsync(ct);

                foreach (var accountId in accounts)
                {
                    dbContext.NotificationEnqueue.Add(new NotificationEnqueueEntity
                    {
                        Id = Guid.NewGuid().ToString("N"),
                        AccountId = accountId,
                        ProductId = existing.ProductId,
                        Checked = false
                    });
                    notificationsEnqueued++;
                }
            }
        }

        await dbContext.SaveChangesAsync(ct);

        var response = new TriggerMatchResponse(
            runId,
            productsProcessed,
            productsCreated,
            pricesUpserted,
            notificationsEnqueued);

        logger.LogInformation(
            "Match processing completed. run_id={RunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
            response.RunId,
            response.ProductsProcessed,
            response.ProductsCreated,
            response.PricesUpserted,
            response.NotificationsEnqueued);

        return response;
    }

    private async Task<string?> ResolveRunIdAsync(DateTime startedAtUtc, DateTime finishedAtUtc, CancellationToken ct)
    {
        var filterExact = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq("started_at", startedAtUtc),
            Builders<BsonDocument>.Filter.Eq("finished_at", finishedAtUtc));

        var exact = await _runs.Find(filterExact).Limit(1).FirstOrDefaultAsync(ct);
        if (exact is not null && exact.TryGetValue("_id", out var exactId))
        {
            return exactId.AsString;
        }

        var tolerance = TimeSpan.FromSeconds(5);
        var filterFuzzy = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Gte("started_at", startedAtUtc.Subtract(tolerance)),
            Builders<BsonDocument>.Filter.Lte("started_at", startedAtUtc.Add(tolerance)),
            Builders<BsonDocument>.Filter.Gte("finished_at", finishedAtUtc.Subtract(tolerance)),
            Builders<BsonDocument>.Filter.Lte("finished_at", finishedAtUtc.Add(tolerance)));

        var fuzzy = await _runs
            .Find(filterFuzzy)
            .Sort(Builders<BsonDocument>.Sort.Descending("finished_at"))
            .Limit(1)
            .FirstOrDefaultAsync(ct);

        if (fuzzy is not null && fuzzy.TryGetValue("_id", out var fuzzyId))
        {
            logger.LogWarning(
                "Resolved scrape run via fuzzy time match. started_at={StartedAt} finished_at={FinishedAt} run_id={RunId}",
                startedAtUtc,
                finishedAtUtc,
                fuzzyId.AsString);
            return fuzzyId.AsString;
        }

        return null;
    }

    private async Task<List<ScrapeProduct>> LoadProductsForRunAsync(string runId, CancellationToken ct)
    {
        var filter = Builders<BsonDocument>.Filter.Eq("run_id", runId);

        var docs = await _products
            .Find(filter)
            .ToListAsync(ct);

        var result = new List<ScrapeProduct>(docs.Count);
        foreach (var doc in docs)
        {
            var externalName = doc.GetValue("external_name", string.Empty).AsString;
            var storeId = doc.GetValue("store_id", string.Empty).AsString;
            if (string.IsNullOrWhiteSpace(externalName) || string.IsNullOrWhiteSpace(storeId))
            {
                continue;
            }

            var price = ReadDecimal(doc, "price");
            var sale = ReadNullableDecimal(doc, "sale");
            var promotionExpiryDate = ReadNullableDate(doc, "promotion_expiry_date");

            result.Add(new ScrapeProduct(
                externalName,
                storeId,
                doc.GetValue("category_id", string.Empty).AsString,
                doc.GetValue("image_url", string.Empty).AsString,
                price,
                sale,
                promotionExpiryDate));
        }

        return result;
    }

    private async Task<PriceEntity?> FindMatchAsync(string name, string storeId, string imageUrl, CancellationToken ct)
    {
        return await dbContext.Prices
            .Join(
                dbContext.Products,
                price => price.ProductId,
                product => product.Id,
                (price, product) => new { price, product })
            .Where(row =>
                row.price.StoreId == storeId &&
                EF.Functions.ILike(row.product.Name, name) &&
                row.product.Image == imageUrl)
            .Select(row => row.price)
            .FirstOrDefaultAsync(ct);
    }

    private async Task<string> ResolveCategoryIdAsync(string? scrapedCategoryId, CancellationToken ct)
    {
        if (!string.IsNullOrWhiteSpace(scrapedCategoryId))
        {
            var exists = dbContext.Categories.Local.Any(category => category.Id == scrapedCategoryId)
                || await dbContext.Categories.AnyAsync(category => category.Id == scrapedCategoryId, ct);

            if (!exists)
            {
                dbContext.Categories.Add(new CategoryEntity { Id = scrapedCategoryId });
            }

            return scrapedCategoryId;
        }

        const string fallbackCategoryId = "cat_uncategorized";
        var fallbackExists = dbContext.Categories.Local.Any(category => category.Id == fallbackCategoryId)
            || await dbContext.Categories.AnyAsync(category => category.Id == fallbackCategoryId, ct);

        if (!fallbackExists)
        {
            dbContext.Categories.Add(new CategoryEntity { Id = fallbackCategoryId });
        }

        return fallbackCategoryId;
    }

    private static DateTime NormalizeToUtc(DateTime value)
    {
        return value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Unspecified => DateTime.SpecifyKind(value, DateTimeKind.Utc),
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => value
        };
    }

    private static bool HasNewOrChangedSale(decimal? before, decimal? after)
    {
        if (!after.HasValue || after.Value <= 0)
        {
            return false;
        }

        if (!before.HasValue)
        {
            return true;
        }

        return before.Value != after.Value;
    }

    private static decimal ReadDecimal(BsonDocument doc, string key)
    {
        var value = doc.GetValue(key, 0m);
        return value switch
        {
            BsonDecimal128 decimal128 => (decimal)decimal128.AsDecimal,
            BsonDouble bsonDouble => (decimal)bsonDouble.AsDouble,
            BsonInt32 bsonInt32 => bsonInt32.AsInt32,
            BsonInt64 bsonInt64 => bsonInt64.AsInt64,
            _ => decimal.TryParse(value.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : 0m
        };
    }

    private static decimal? ReadNullableDecimal(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value.IsBsonNull)
        {
            return null;
        }

        return ReadDecimal(doc, key);
    }

    private static DateTime? ReadNullableDate(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value.IsBsonNull)
        {
            return null;
        }

        var raw = value.ToString();
        if (DateOnly.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateOnly))
        {
            return dateOnly.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        }

        return DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed)
            ? parsed.ToUniversalTime()
            : null;
    }

    private sealed record ScrapeProduct(
        string ExternalName,
        string StoreId,
        string CategoryId,
        string? ImageUrl,
        decimal Price,
        decimal? Sale,
        DateTime? PromotionExpiryDate);
}
