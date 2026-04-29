using MatchQueueService.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace MatchQueueService.Data;

public sealed class MatchQueueDbContext(DbContextOptions<MatchQueueDbContext> options) : DbContext(options)
{
    public DbSet<CategoryEntity> Categories => Set<CategoryEntity>();
    public DbSet<ProductEntity> Products => Set<ProductEntity>();
    public DbSet<PriceEntity> Prices => Set<PriceEntity>();
    public DbSet<FavoriteProductEntity> FavoriteProducts => Set<FavoriteProductEntity>();
    public DbSet<NotificationEnqueueEntity> NotificationEnqueue => Set<NotificationEnqueueEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CategoryEntity>(entity =>
        {
            entity.ToTable("categories");
            entity.HasKey(category => category.Id);

            entity.Property(category => category.Id)
                .HasColumnName("id")
                .HasColumnType("varchar");
        });

        modelBuilder.Entity<ProductEntity>(entity =>
        {
            entity.ToTable("products");
            entity.HasKey(product => product.Id);

            entity.Property(product => product.Id)
                .HasColumnName("id")
                .HasColumnType("varchar");

            entity.Property(product => product.Name)
                .HasColumnName("name")
                .HasColumnType("varchar")
                .IsRequired();

            entity.Property(product => product.Barcode)
                .HasColumnName("barcode")
                .HasColumnType("varchar")
                .IsRequired();

            entity.Property(product => product.CategoryId)
                .HasColumnName("category_id")
                .HasColumnType("varchar")
                .IsRequired();

            entity.Property(product => product.Image)
                .HasColumnName("image")
                .HasColumnType("varchar")
                .IsRequired();
        });

        modelBuilder.Entity<PriceEntity>(entity =>
        {
            entity.ToTable("prices");
            entity.HasKey(price => price.Id);

            entity.Property(price => price.Id)
                .HasColumnName("id")
                .HasColumnType("varchar");

            entity.Property(price => price.ProductId)
                .HasColumnName("product_id")
                .HasColumnType("varchar")
                .IsRequired();

            entity.Property(price => price.StoreId)
                .HasColumnName("store_id")
                .HasColumnType("varchar")
                .IsRequired();

            entity.Property(price => price.Price)
                .HasColumnName("price")
                .HasColumnType("numeric(10,2)")
                .IsRequired();

            entity.Property(price => price.Sale)
                .HasColumnName("sale")
                .HasColumnType("numeric(10,2)");

            entity.Property(price => price.SaleDate)
                .HasColumnName("sale_date")
                .HasColumnType("timestamp with time zone");

            entity.Property(price => price.UpdatedAt)
                .HasColumnName("updated_at")
                .HasColumnType("timestamp with time zone")
                .IsRequired();

            entity.HasIndex(price => new { price.ProductId, price.StoreId })
                .IsUnique()
                .HasDatabaseName("UX_prices_product_store");
        });

        modelBuilder.Entity<FavoriteProductEntity>(entity =>
        {
            entity.ToTable("favorite_products");
            entity.HasKey(favorite => new { favorite.AccountId, favorite.ProductId });

            entity.Property(favorite => favorite.AccountId)
                .HasColumnName("account_id")
                .HasColumnType("varchar(128)")
                .IsRequired();

            entity.Property(favorite => favorite.ProductId)
                .HasColumnName("product_id")
                .HasColumnType("varchar(128)")
                .IsRequired();
        });

        modelBuilder.Entity<NotificationEnqueueEntity>(entity =>
        {
            entity.ToTable("notification_enque");
            entity.HasKey(notification => notification.Id);

            entity.Property(notification => notification.Id)
                .HasColumnName("id")
                .HasColumnType("varchar(128)");

            entity.Property(notification => notification.AccountId)
                .HasColumnName("account_id")
                .HasColumnType("varchar(128)")
                .IsRequired();

            entity.Property(notification => notification.ProductId)
                .HasColumnName("product_id")
                .HasColumnType("varchar(128)")
                .IsRequired();

            entity.Property(notification => notification.Checked)
                .HasColumnName("checked")
                .HasDefaultValue(false);
        });
    }
}
