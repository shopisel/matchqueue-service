using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MatchQueueService.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialBaseline : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // Baseline migration: schema already exists in the shared database.
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            // No-op.
        }
    }
}
