using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MatchQueueService.Data.Migrations
{
    /// <inheritdoc />
    public partial class CreateNotificationEnqueue : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "notification_enque",
                columns: table => new
                {
                    id = table.Column<string>(type: "varchar(128)", nullable: false),
                    account_id = table.Column<string>(type: "varchar(128)", nullable: false),
                    product_id = table.Column<string>(type: "varchar(128)", nullable: false),
                    @checked = table.Column<bool>(name: "checked", type: "boolean", nullable: false, defaultValue: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_notification_enque", x => x.id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "notification_enque");
        }
    }
}
