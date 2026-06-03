using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
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
    private static readonly Regex TokenRegex = new(@"[A-Z0-9.]+", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex MultipleWhitespaceRegex = new(@"\s+", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex QuantityTokenRegex = new(@"^(?<value>\d+(?:\.\d+)?)(?<unit>ML|CL|L|G|KG|MG|GR|UN|UNI|UND)$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex PackQuantityRegex = new(@"^(?<count>\d+(?:\.\d+)?)X(?<value>\d+(?:\.\d+)?)(?<unit>ML|CL|L|G|KG|MG|GR)$", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex ZeroSugarRegex = new(@"\b(ZERO|SEM|S)\s+ACUCAR\b", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> StopWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "NOVO",
        "PROMO",
        "PROMOCAO",
        "PACK",
        "ORIGINAL",
        "CLASSICO",
        "CLASSICA",
        "FAMILIAR",
        "LATA",
        "GARRAFA",
        "PET",
        "EMBALAGEM",
        "SABOR",
        "EDICAO",
        "LIMITADA"
    };

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
        var productsByCategoryId = new Dictionary<string, List<ProductEntity>>(StringComparer.OrdinalIgnoreCase);
        var allProducts = await dbContext.Products.ToListAsync(ct);
        var allPrices = await dbContext.Prices.ToListAsync(ct);
        var knownCategoryIds = await LoadKnownCategoryIdsAsync(ct);

        foreach (var existingProduct in allProducts)
        {
            if (!productsByCategoryId.TryGetValue(existingProduct.CategoryId, out var categoryProducts))
            {
                categoryProducts = [];
                productsByCategoryId[existingProduct.CategoryId] = categoryProducts;
            }

            categoryProducts.Add(existingProduct);

            if (!string.IsNullOrWhiteSpace(existingProduct.CanonicalKey))
            {
                productsByCanonicalKey[existingProduct.CanonicalKey] = existingProduct;
            }
        }

        foreach (var existingPrice in allPrices)
        {
            pricesByProductAndStore[BuildPriceKey(existingPrice.ProductId, existingPrice.StoreId)] = existingPrice;
        }

        var previousAutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
        dbContext.ChangeTracker.AutoDetectChangesEnabled = false;

        try
        {
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

                var canonicalKey = BuildCanonicalKey(name, brand, product.QuantityText);
                if (!productsByCanonicalKey.TryGetValue(canonicalKey, out var productEntity))
                {
                    productEntity = FindProductBySignature(
                        productsByCategoryId,
                        allProducts,
                        product.CategoryId,
                        name,
                        brand,
                        product.QuantityText);

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
                    var categoryId = ResolveCategoryId(product.CategoryId, knownCategoryIds);

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
                    allProducts.Add(productEntity);

                    productsCreated++;
                }

                var priceKey = BuildPriceKey(productEntity.Id, storeId);
                if (!pricesByProductAndStore.TryGetValue(priceKey, out var existingPrice))
                {
                    existingPrice = null;
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
                    allPrices.Add(existingPrice);
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
        }

        finally
        {
            dbContext.ChangeTracker.AutoDetectChangesEnabled = previousAutoDetectChangesEnabled;
        }

        dbContext.ChangeTracker.DetectChanges();
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

    private static ProductEntity? FindProductBySignature(
        IReadOnlyDictionary<string, List<ProductEntity>> productsByCategoryId,
        IReadOnlyList<ProductEntity> allProducts,
        string? categoryId,
        string name,
        string? brand,
        string? quantityText)
    {
        var canonicalKey = BuildCanonicalKey(name, brand, quantityText);

        if (!string.IsNullOrWhiteSpace(categoryId)
            && productsByCategoryId.TryGetValue(categoryId, out var categoryCandidates))
        {
            var categoryMatch = categoryCandidates.FirstOrDefault(product =>
                BuildCanonicalKey(product.Name, product.Brand, null) == canonicalKey);

            if (categoryMatch is not null)
            {
                return categoryMatch;
            }
        }

        return allProducts.FirstOrDefault(product =>
            BuildCanonicalKey(product.Name, product.Brand, null) == canonicalKey);
    }

    private async Task<HashSet<string>> LoadKnownCategoryIdsAsync(CancellationToken ct)
    {
        var categories = await dbContext.Categories
            .Select(category => category.Id)
            .ToListAsync(ct);

        return new HashSet<string>(categories, StringComparer.OrdinalIgnoreCase);
    }

    private string ResolveCategoryId(string? scrapedCategoryId, ISet<string> knownCategoryIds)
    {
        if (!string.IsNullOrWhiteSpace(scrapedCategoryId))
        {
            if (knownCategoryIds.Add(scrapedCategoryId))
            {
                dbContext.Categories.Add(new CategoryEntity { Id = scrapedCategoryId });
            }

            return scrapedCategoryId;
        }

        const string fallbackCategoryId = "cat_uncategorized";
        if (knownCategoryIds.Add(fallbackCategoryId))
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

    private static string BuildCanonicalKey(string name, string? brand, string? quantityText = null)
    {
        var tokens = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

        AddMatchTokens(tokens, name);
        AddMatchTokens(tokens, brand);
        AddMatchTokens(tokens, quantityText);

        return string.Join("|", tokens);
    }

    private static string BuildPriceKey(string productId, string storeId)
    {
        return string.Join("|", productId.Trim(), storeId.Trim());
    }

    private static void AddMatchTokens(ISet<string> tokens, string? value)
    {
        foreach (var token in TokenizeMatchComponent(value))
        {
            tokens.Add(token);
        }
    }

    private static IEnumerable<string> TokenizeMatchComponent(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            yield break;
        }

        var normalized = value.Trim().Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(normalized.Length);

        foreach (var character in normalized)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(character) != UnicodeCategory.NonSpacingMark)
            {
                builder.Append(character);
            }
        }

        var withoutDiacritics = builder.ToString().Normalize(NormalizationForm.FormC).ToUpperInvariant();
        withoutDiacritics = withoutDiacritics.Replace(',', '.');
        withoutDiacritics = ZeroSugarRegex.Replace(withoutDiacritics, "SEM ACUCAR");
        withoutDiacritics = MultipleWhitespaceRegex.Replace(withoutDiacritics, " ");

        var rawTokens = TokenRegex.Matches(withoutDiacritics)
            .Select(match => match.Value)
            .ToArray();

        for (var index = 0; index < rawTokens.Length; index++)
        {
            if (TryReadSugarToken(rawTokens, index, out var sugarToken, out var sugarConsumed))
            {
                yield return sugarToken;
                index += sugarConsumed - 1;
                continue;
            }

            if (TryReadPackQuantityToken(rawTokens, index, out var packQuantityToken, out var packQuantityConsumed))
            {
                yield return packQuantityToken;
                index += packQuantityConsumed - 1;
                continue;
            }

            var token = rawTokens[index];
            if (StopWords.Contains(token))
            {
                continue;
            }

            if (TryReadQuantityToken(token, out var quantityToken))
            {
                yield return quantityToken;
                continue;
            }

            if (token.Length == 1 && !char.IsDigit(token[0]))
            {
                continue;
            }

            yield return token;
        }
    }

    private static bool TryReadSugarToken(IReadOnlyList<string> tokens, int index, out string token, out int consumedTokens)
    {
        token = string.Empty;
        consumedTokens = 0;

        if (index + 1 >= tokens.Count)
        {
            return false;
        }

        var first = tokens[index];
        var second = tokens[index + 1];

        if ((first == "ZERO" || first == "SEM" || first == "S") && second == "ACUCAR")
        {
            token = "SEM_ACUCAR";
            consumedTokens = 2;
            return true;
        }

        return false;
    }

    private static bool TryReadPackQuantityToken(IReadOnlyList<string> tokens, int index, out string token, out int consumedTokens)
    {
        token = string.Empty;
        consumedTokens = 0;

        if (index + 2 >= tokens.Count)
        {
            return false;
        }

        if (!TryNormalizeDecimalToken(tokens[index], out var packCount))
        {
            return false;
        }

        if (tokens[index + 1] != "X")
        {
            return false;
        }

        if (TryReadQuantityToken(tokens[index + 2], out var packedQuantity))
        {
            token = $"{packCount}X{packedQuantity}";
            consumedTokens = 3;
            return true;
        }

        if (index + 3 < tokens.Count &&
            TryNormalizeDecimalToken(tokens[index + 2], out var quantityValue) &&
            IsQuantityUnitToken(tokens[index + 3], out var unit))
        {
            token = $"{packCount}X{quantityValue}{unit}";
            consumedTokens = 4;
            return true;
        }

        return false;
    }

    private static bool TryReadQuantityToken(string token, out string canonicalToken)
    {
        canonicalToken = string.Empty;

        var match = QuantityTokenRegex.Match(token.Replace(',', '.'));
        if (!match.Success)
        {
            var packMatch = PackQuantityRegex.Match(token.Replace(',', '.'));
            if (!packMatch.Success)
            {
                return false;
            }

            canonicalToken = $"{NormalizeDecimalPart(packMatch.Groups["count"].Value)}X{NormalizeDecimalPart(packMatch.Groups["value"].Value)}{packMatch.Groups["unit"].Value}";
            return true;
        }

        canonicalToken = $"{NormalizeDecimalPart(match.Groups["value"].Value)}{match.Groups["unit"].Value}";
        return true;
    }

    private static bool TryNormalizeDecimalToken(string token, out string normalizedToken)
    {
        normalizedToken = string.Empty;

        if (!decimal.TryParse(token.Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed))
        {
            return false;
        }

        normalizedToken = parsed % 1 == 0
            ? parsed.ToString("0", CultureInfo.InvariantCulture)
            : parsed.ToString(CultureInfo.InvariantCulture);
        return true;
    }

    private static bool IsQuantityUnitToken(string token, out string unit)
    {
        var normalized = token.Trim().ToUpperInvariant();
        if (normalized is "ML" or "CL" or "L" or "G" or "KG" or "MG" or "GR" or "UN" or "UNI" or "UND")
        {
            unit = normalized;
            return true;
        }

        unit = string.Empty;
        return false;
    }

    private static string NormalizeDecimalPart(string value)
    {
        if (decimal.TryParse(value.Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed % 1 == 0
                ? parsed.ToString("0", CultureInfo.InvariantCulture)
                : parsed.ToString(CultureInfo.InvariantCulture);
        }

        return value.Replace(',', '.');
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
