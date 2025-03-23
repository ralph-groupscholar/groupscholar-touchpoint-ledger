//! Shared constants for the touchpoint ledger CLI.
const std = @import("std");

pub const schema_name = "touchpoint_ledger";

pub fn schemaQualified(table: []const u8) []const u8 {
    return schema_name ++ "." ++ table;
}

test "schema name constant" {
    try std.testing.expectEqualStrings("touchpoint_ledger", schema_name);
}
