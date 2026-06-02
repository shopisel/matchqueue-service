using System.Globalization;
using System.Text;
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
        return await ProcessRunProductsAsync(runId, scrapedProducts, ct);
    }

    public async Task<TriggerMatchResponse> ProcessRunAsync(string runId, CancellationToken ct)
    {
        runId = (runId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(runId))
        {
            throw new ArgumentException("runId is required.", nameof(runId));
        }

        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["run_id"] = runId
        });

        logger.LogInformation("Match processing started (direct run_id).");

        var scrapedProducts = await LoadProductsForRunAsync(runId, ct);
        return await ProcessRunProductsAsync(runId, scrapedProducts, ct);
    }

    public async Task<TriggerMatchResponse> ProcessWorkerRunAsync(string workerRunId, CancellationToken ct)
    {
        workerRunId = (workerRunId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(workerRunId))
        {
            throw new ArgumentException("workerRunId is required.", nameof(workerRunId));
        }

        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["worker_run_id"] = workerRunId
        });

        logger.LogInformation("Match processing started (worker_run_id).");

        var runIds = await _runs
            .Find(Builders<BsonDocument>.Filter.Eq("worker_run_id", workerRunId))
            .Sort(Builders<BsonDocument>.Sort.Ascending("started_at"))
            .Project(Builders<BsonDocument>.Projection.Include("_id"))
            .ToListAsync(ct);

        var resolvedRunIds = runIds
            .Select(doc => (doc.GetValue("_id", BsonNull.Value).ToString() ?? string.Empty).Trim())
            .Where(id => id.Length > 0 && !id.Equals("BsonNull", StringComparison.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (resolvedRunIds.Count == 0)
        {
            logger.LogWarning("No scrape runs found for worker_run_id={WorkerRunId}.", workerRunId);
            return new TriggerMatchResponse(workerRunId, 0, 0, 0, 0);
        }

        var totalProcessed = 0;
        var totalCreated = 0;
        var totalPricesUpserted = 0;
        var totalNotificationsEnqueued = 0;

        foreach (var runId in resolvedRunIds)
        {
            ct.ThrowIfCancellationRequested();

            var runResult = await ProcessRunAsync(runId, ct);
            totalProcessed += runResult.ProductsProcessed;
            totalCreated += runResult.ProductsCreated;
            totalPricesUpserted += runResult.PricesUpserted;
            totalNotificationsEnqueued += runResult.NotificationsEnqueued;
        }

        var response = new TriggerMatchResponse(
            workerRunId,
            totalProcessed,
            totalCreated,
            totalPricesUpserted,
            totalNotificationsEnqueued);

        logger.LogInformation(
            "Match processing completed (worker_run_id). worker_run_id={WorkerRunId} processed={Processed} created={Created} prices_upserted={PricesUpserted} notifications_enqueued={NotificationsEnqueued}",
            workerRunId,
            response.ProductsProcessed,
            response.ProductsCreated,
            response.PricesUpserted,
            response.NotificationsEnqueued);

        return response;
    }

    private async Task<TriggerMatchResponse> ProcessRunProductsAsync(
        string runId,
        List<ScrapeProduct> scrapedProducts,
        CancellationToken ct)
    {
        if (scrapedProducts.Count == 0)
        {
            logger.LogWarning("No scrape products found for run_id={RunId}.", runId);
            return new TriggerMatchResponse(runId, 0, 0, 0, 0);
        }

        var productsProcessed = 0;
        var productsCreated = 0;
        var pricesUpserted = 0;
        var notificationsEnqueued = 0;
        var productsByCanonicalKey = new Dictionary<string, ProductEntity>(StringComparer.OrdinalIgnoreCase);
        var pricesByProductAndStore = new Dictionary<string, PriceEntity>(StringComparer.OrdinalIgnoreCase);

        foreach (var product in scrapedProducts)
        {
            ct.ThrowIfCancellationRequested();
            productsProcessed++;

            var name = product.ExternalName.Trim();
            var brand = string.IsNullOrWhiteSpace(product.Brand) ? null : product.Brand.Trim();
            var storeId = product.StoreId.Trim();
            var imageUrl = (product.ImageUrl ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(storeId))
            {
                continue;
            }

            var canonicalKey = BuildCanonicalKey(name, brand);
            if (!productsByCanonicalKey.TryGetValue(canonicalKey, out var productEntity))
            {
                productEntity = await FindProductByCanonicalKeyAsync(canonicalKey, ct)
                    ?? await FindLegacyProductMatchAsync(name, brand, ct);

                if (productEntity is not null)
                {
                    if (string.IsNullOrWhiteSpace(productEntity.CanonicalKey))
                    {
                        productEntity.CanonicalKey = canonicalKey;
                    }

                    productsByCanonicalKey[canonicalKey] = productEntity;
                }
            }

            if (productEntity is null)
            {
                var categoryId = await ResolveCategoryIdAsync(product.CategoryId, ct);

                productEntity = new ProductEntity
                {
                    Id = Guid.NewGuid().ToString("N"),
                    CanonicalKey = canonicalKey,
                    Name = name,
                    Brand = brand,
                    Barcode = string.Empty,
                    CategoryId = categoryId,
                    Image = imageUrl
                };

                dbContext.Products.Add(productEntity);
                productsByCanonicalKey[canonicalKey] = productEntity;

                productsCreated++;
            }

            var priceKey = BuildPriceKey(productEntity.Id, storeId);
            if (!pricesByProductAndStore.TryGetValue(priceKey, out var existingPrice))
            {
                existingPrice = await FindPriceAsync(productEntity.Id, storeId, ct);
                if (existingPrice is not null)
                {
                    pricesByProductAndStore[priceKey] = existingPrice;
                }
            }

            if (existingPrice is null)
            {
                existingPrice = new PriceEntity
                {
                    Id = Guid.NewGuid().ToString("N"),
                    ProductId = productEntity.Id,
                    StoreId = storeId,
                    PriceText = product.PriceText,
                    SaleText = product.SaleText,
                    QuantityText = product.QuantityText,
                    UnitPriceText = product.UnitPriceText,
                    SaleDate = product.PromotionExpiryDate,
                    UpdatedAt = DateTime.UtcNow
                };

                dbContext.Prices.Add(existingPrice);
                pricesByProductAndStore[priceKey] = existingPrice;
                pricesUpserted++;
                continue;
            }

            var previousSaleText = existingPrice.SaleText;

            existingPrice.PriceText = product.PriceText;
            existingPrice.SaleText = product.SaleText;
            existingPrice.QuantityText = product.QuantityText;
            existingPrice.UnitPriceText = product.UnitPriceText;
            existingPrice.SaleDate = product.PromotionExpiryDate;
            existingPrice.UpdatedAt = DateTime.UtcNow;

            pricesUpserted++;

            if (HasNewOrChangedSale(previousSaleText, existingPrice.SaleText))
            {
                var accounts = await dbContext.FavoriteProducts
                    .Where(favorite => favorite.ProductId == productEntity.Id)
                    .Select(favorite => favorite.AccountId)
                    .ToListAsync(ct);

                foreach (var accountId in accounts)
                {
                    dbContext.NotificationEnqueue.Add(new NotificationEnqueueEntity
                    {
                        Id = Guid.NewGuid().ToString("N"),
                        AccountId = accountId,
                        ProductId = productEntity.Id,
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

            var priceText = (doc.GetValue("price_text", string.Empty).ToString() ?? string.Empty).Trim();
            var saleText = ReadNullableString(doc, "sale_text");
            var quantityText = ReadNullableString(doc, "quantity_text");
            var unitPriceText = ReadNullableString(doc, "unit_price_text");
            var brand = ReadNullableString(doc, "brand");
            var promotionExpiryDate = ReadNullableDate(doc, "promotion_expiry_date");

            result.Add(new ScrapeProduct(
                externalName,
                storeId,
                doc.GetValue("category_id", string.Empty).AsString,
                doc.GetValue("image_url", string.Empty).AsString,
                priceText,
                saleText,
                quantityText,
                unitPriceText,
                brand,
                promotionExpiryDate));
        }

        return result;
    }

    private async Task<ProductEntity?> FindProductByCanonicalKeyAsync(string canonicalKey, CancellationToken ct)
    {
        return await dbContext.Products
            .FirstOrDefaultAsync(product => product.CanonicalKey == canonicalKey, ct);
    }

    private async Task<ProductEntity?> FindLegacyProductMatchAsync(string name, string? brand, CancellationToken ct)
    {
        var normalizedName = NormalizeMatchComponent(name);
        var normalizedBrand = NormalizeMatchComponent(brand);

        var query = dbContext.Products.Where(product => EF.Functions.ILike(product.Name, name));

        if (string.IsNullOrWhiteSpace(brand))
        {
            query = query.Where(product => string.IsNullOrWhiteSpace(product.Brand));
        }
        else
        {
            query = query.Where(product => product.Brand != null && EF.Functions.ILike(product.Brand, brand));
        }

        var candidates = await query.ToListAsync(ct);

        return candidates.FirstOrDefault(product =>
            NormalizeMatchComponent(product.Name) == normalizedName &&
            NormalizeMatchComponent(product.Brand) == normalizedBrand);
    }

    private async Task<PriceEntity?> FindPriceAsync(string productId, string storeId, CancellationToken ct)
    {
        return await dbContext.Prices
            .FirstOrDefaultAsync(price => price.ProductId == productId && price.StoreId == storeId, ct);
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

    private static string BuildCanonicalKey(string name, string? brand)
    {
        return string.Join("|",
            NormalizeMatchComponent(name),
            NormalizeMatchComponent(brand));
    }

    private static string BuildPriceKey(string productId, string storeId)
    {
        return string.Join("|", productId.Trim(), storeId.Trim());
    }

    private static string NormalizeMatchComponent(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var trimmed = value.Trim().Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(trimmed.Length);

        foreach (var character in trimmed)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(character) != UnicodeCategory.NonSpacingMark)
            {
                builder.Append(character);
            }
        }

        var withoutDiacritics = builder.ToString().Normalize(NormalizationForm.FormC);
        var collapsedWhitespace = string.Join(' ', withoutDiacritics.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        return collapsedWhitespace.ToUpperInvariant();
    }

    private static bool HasNewOrChangedSale(string? beforeSaleText, string? afterSaleText)
    {
        var before = TryReadEuroDecimal(beforeSaleText);
        var after = TryReadEuroDecimal(afterSaleText);

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

    private static decimal? TryReadEuroDecimal(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        var euroIndex = trimmed.IndexOf('€');
        var numberPart = euroIndex >= 0 ? trimmed[..euroIndex] : trimmed;
        numberPart = numberPart.Trim();

        if (numberPart.Length == 0)
        {
            return null;
        }

        if (decimal.TryParse(numberPart, NumberStyles.Number, CultureInfo.GetCultureInfo("pt-PT"), out var parsedPt))
        {
            return parsedPt;
        }

        return decimal.TryParse(numberPart, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsedInvariant)
            ? parsedInvariant
            : null;
    }

    private static string? ReadNullableString(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value.IsBsonNull)
        {
            return null;
        }

        var text = (value.ToString() ?? string.Empty).Trim();
        return text.Length == 0 ? null : text;
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
        string PriceText,
        string? SaleText,
        string? QuantityText,
        string? UnitPriceText,
        string? Brand,
        DateTime? PromotionExpiryDate);
}
