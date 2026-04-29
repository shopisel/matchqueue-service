using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace MatchQueueService.Data;

public sealed class MatchQueueDbContextFactory : IDesignTimeDbContextFactory<MatchQueueDbContext>
{
    public MatchQueueDbContext CreateDbContext(string[] args)
    {
        var connectionString =
            Environment.GetEnvironmentVariable("MATCHQUEUE_CONNECTION_STRING") ??
            "Host=localhost;Database=shopisel;Username=postgres;Password=postgres";

        var optionsBuilder = new DbContextOptionsBuilder<MatchQueueDbContext>();
        optionsBuilder.UseNpgsql(connectionString);

        return new MatchQueueDbContext(optionsBuilder.Options);
    }
}

