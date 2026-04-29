using System.Text.Json;
using System.Text.Json.Serialization;

namespace MatchQueueService.Contracts;

[JsonConverter(typeof(TriggerMatchRequestJsonConverter))]
public sealed record TriggerMatchRequest(
    DateTime StartedAt,
    DateTime FinishedAt);

internal sealed class TriggerMatchRequestJsonConverter : JsonConverter<TriggerMatchRequest>
{
    public override TriggerMatchRequest Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected JSON object.");
        }

        DateTime? startedAt = null;
        DateTime? finishedAt = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                continue;
            }

            var propertyName = reader.GetString();
            reader.Read();

            if (propertyName is "startedAt" or "started_at")
            {
                startedAt = reader.TokenType == JsonTokenType.String
                    ? reader.GetDateTime()
                    : JsonSerializer.Deserialize<DateTime>(ref reader, options);
                continue;
            }

            if (propertyName is "finishedAt" or "finished_at")
            {
                finishedAt = reader.TokenType == JsonTokenType.String
                    ? reader.GetDateTime()
                    : JsonSerializer.Deserialize<DateTime>(ref reader, options);
            }
            else
            {
                reader.Skip();
            }
        }

        if (!startedAt.HasValue || !finishedAt.HasValue)
        {
            throw new JsonException("Missing required properties: startedAt/started_at and finishedAt/finished_at.");
        }

        return new TriggerMatchRequest(startedAt.Value, finishedAt.Value);
    }

    public override void Write(Utf8JsonWriter writer, TriggerMatchRequest value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteString("startedAt", value.StartedAt);
        writer.WriteString("finishedAt", value.FinishedAt);
        writer.WriteEndObject();
    }
}
