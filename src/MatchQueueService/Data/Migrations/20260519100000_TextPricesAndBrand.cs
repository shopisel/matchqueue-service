using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MatchQueueService.Data.Migrations;

public partial class TextPricesAndBrand : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "brand",
            table: "products",
            type: "varchar",
            nullable: true);

        migrationBuilder.RenameColumn(
            name: "price",
            table: "prices",
            newName: "price_text");

        migrationBuilder.RenameColumn(
            name: "sale",
            table: "prices",
            newName: "sale_text");

        // Convert existing numeric values to text.
        migrationBuilder.Sql("ALTER TABLE prices ALTER COLUMN price_text TYPE varchar USING price_text::text;");
        migrationBuilder.Sql("ALTER TABLE prices ALTER COLUMN sale_text TYPE varchar USING sale_text::text;");

        migrationBuilder.AddColumn<string>(
            name: "quantity_text",
            table: "prices",
            type: "varchar",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "unit_price_text",
            table: "prices",
            type: "varchar",
            nullable: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "brand",
            table: "products");

        migrationBuilder.DropColumn(
            name: "quantity_text",
            table: "prices");

        migrationBuilder.DropColumn(
            name: "unit_price_text",
            table: "prices");

        // Best-effort conversion back to numeric(10,2) from text.
        migrationBuilder.Sql(
            "ALTER TABLE prices ALTER COLUMN price_text TYPE numeric(10,2) USING NULLIF(REPLACE(REGEXP_REPLACE(price_text, '[^0-9,\\.-]', '', 'g'), ',', '.'), '')::numeric(10,2);");
        migrationBuilder.Sql(
            "ALTER TABLE prices ALTER COLUMN sale_text TYPE numeric(10,2) USING NULLIF(REPLACE(REGEXP_REPLACE(sale_text, '[^0-9,\\.-]', '', 'g'), ',', '.'), '')::numeric(10,2);");

        migrationBuilder.RenameColumn(
            name: "price_text",
            table: "prices",
            newName: "price");

        migrationBuilder.RenameColumn(
            name: "sale_text",
            table: "prices",
            newName: "sale");
    }
}

