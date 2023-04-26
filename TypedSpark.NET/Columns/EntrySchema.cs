namespace TypedSpark.NET.Columns;

/// <summary>
/// Entry schema
/// </summary>
public sealed class EntrySchema<TKey, TValue> : TypedSchema<EntrySchema<TKey, TValue>>
{
    /// <summary>
    /// Key
    /// </summary>
    [SparkName("key")]
    public TKey Key { get; private set; } = default!;

    /// <summary>
    /// Value
    /// </summary>
    [SparkName("value")]
    public TValue Value { get; private set; } = default!;

    public EntrySchema(string? alias)
        : base(alias) { }

    public EntrySchema()
        : base(default) { }
}
