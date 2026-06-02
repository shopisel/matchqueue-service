using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MatchQueueService.Data.Migrations;

public partial class AddProductCanonicalKey : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "canonical_key",
            table: "products",
            type: "varchar",
            nullable: true);

        migrationBuilder.CreateIndex(
            name: "UX_products_canonical_key",
            table: "products",
            column: "canonical_key",
            unique: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "UX_products_canonical_key",
            table: "products");

        migrationBuilder.DropColumn(
            name: "canonical_key",
            table: "products");
    }
}
