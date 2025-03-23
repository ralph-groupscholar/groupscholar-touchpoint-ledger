const std = @import("std");

const usage =
    \\\\Group Scholar Touchpoint Ledger
    \\\\Usage:
    \\\\  gs-touchpoint-ledger <command> [options]
    \\\\
    \\\\Commands:
    \\\\  init-db                 Create schema and seed production data
    \\\\  log                     Record a scholar touchpoint
    \\\\  list                    List recent touchpoints
    \\\\  weekly                  Summarize touchpoints for a week
    \\\\  follow-ups              List upcoming follow-ups
    \\\\  staff-summary           Summarize touchpoints by staff
    \\\\
    \\\\Global options:
    \\\\  --dry-run               Print SQL instead of executing
    \\\\
    \\\\log options:
    \\\\  --scholar <name>        Scholar name (required)
    \\\\  --scholar-id <id>       Scholar identifier
    \\\\  --channel <channel>     email | call | sms | meeting | other (required)
    \\\\  --staff <name>          Staff member (required)
    \\\\  --notes <text>          Notes
    \\\\  --follow-up <date>      Follow-up date (YYYY-MM-DD)
    \\\\  --occurred <ts>         Occurred timestamp (YYYY-MM-DD or ISO8601)
    \\\\
    \\\\list options:
    \\\\  --limit <n>             Result limit (default 20)
    \\\\  --since <date>          Only show touchpoints since date
    \\\\
    \\\\weekly options:
    \\\\  --week-start <date>     Week start date (YYYY-MM-DD)
    \\\\
    \\\\follow-ups options:
    \\\\  --since <date>          Start date (YYYY-MM-DD, default current_date)
    \\\\  --days <n>              Window size in days (default 14)
    \\\\
    \\\\staff-summary options:
    \\\\  --until <date>          Window end date (YYYY-MM-DD, default current_date)
    \\\\  --days <n>              Window size in days (default 30)
    \\\\  --limit <n>             Result limit (default 10)
    \\\\
    \\\\Environment:
    \\\\  GS_TOUCHPOINT_DB_URL    Production Postgres connection URL
    \\\\
    ;

const ParsedArgs = struct {
    options: std.StringHashMap([]const u8),
    positionals: std.ArrayList([]const u8),
    dry_run: bool,

    fn deinit(self: *ParsedArgs, allocator: std.mem.Allocator) void {
        self.options.deinit();
        self.positionals.deinit();
        _ = allocator;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args_it = try std.process.argsWithAllocator(allocator);
    defer args_it.deinit();

    var args = std.ArrayList([]const u8).empty;
    defer args.deinit(allocator);

    while (args_it.next()) |arg| {
        try args.append(allocator, arg);
    }

    if (args.items.len < 2) {
        try printUsage();
        return;
    }

    const cmd = args.items[1];
    const rest = args.items[2..];

    if (std.mem.eql(u8, cmd, "--help") or std.mem.eql(u8, cmd, "help")) {
        try printUsage();
        return;
    }

    var parsed = try parseArgs(allocator, rest);
    defer parsed.deinit(allocator);

    if (std.mem.eql(u8, cmd, "init-db")) {
        try runInitDb(allocator, &parsed);
    } else if (std.mem.eql(u8, cmd, "log")) {
        try runLog(allocator, &parsed);
    } else if (std.mem.eql(u8, cmd, "list")) {
        try runList(allocator, &parsed);
    } else if (std.mem.eql(u8, cmd, "weekly")) {
        try runWeekly(allocator, &parsed);
    } else if (std.mem.eql(u8, cmd, "follow-ups")) {
        try runFollowups(allocator, &parsed);
    } else if (std.mem.eql(u8, cmd, "staff-summary")) {
        try runStaffSummary(allocator, &parsed);
    } else {
        try std.io.getStdErr().writer().print("Unknown command: {s}\n\n", .{cmd});
        try printUsage();
    }
}

fn printUsage() !void {
    try std.io.getStdOut().writer().writeAll(usage);
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !ParsedArgs {
    var options = std.StringHashMap([]const u8).init(allocator);
    var positionals = std.ArrayList([]const u8).empty;

    var dry_run = false;
    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.startsWith(u8, arg, "--")) {
            const key = arg[2..];
            if (std.mem.eql(u8, key, "dry-run")) {
                dry_run = true;
                i += 1;
                continue;
            }
            if (i + 1 >= args.len) return error.MissingValue;
            const value = args[i + 1];
            if (std.mem.startsWith(u8, value, "--")) return error.MissingValue;
            try options.put(key, value);
            i += 2;
            continue;
        }
        try positionals.append(allocator, arg);
        i += 1;
    }

    return ParsedArgs{
        .options = options,
        .positionals = positionals,
        .dry_run = dry_run,
    };
}

fn requireOption(parsed: *const ParsedArgs, key: []const u8) ![]const u8 {
    if (parsed.options.get(key)) |value| return value;
    return error.MissingOption;
}

fn runInitDb(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const schema_path = "sql/01_schema.sql";
    const seed_path = "sql/02_seed.sql";

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print(
            "Would run:\n  psql $GS_TOUCHPOINT_DB_URL -v ON_ERROR_STOP=1 -f {s}\n  psql $GS_TOUCHPOINT_DB_URL -v ON_ERROR_STOP=1 -f {s}\n",
            .{ schema_path, seed_path },
        );
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlFile(allocator, db_url, schema_path);
    try runPsqlFile(allocator, db_url, seed_path);

    try std.io.getStdOut().writer().writeAll("Database initialized and seeded.\n");
}

fn runLog(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const scholar = try requireOption(parsed, "scholar");
    const channel = try requireOption(parsed, "channel");
    const staff = try requireOption(parsed, "staff");

    const scholar_id = parsed.options.get("scholar-id");
    const notes = parsed.options.get("notes");
    const follow_up = parsed.options.get("follow-up");
    const occurred = parsed.options.get("occurred");

    const sql = try buildInsertSql(allocator, scholar, scholar_id, channel, staff, notes, follow_up, occurred);
    defer allocator.free(sql);

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print("{s}\n", .{sql});
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlCommand(allocator, db_url, sql);
}

fn runList(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const limit_raw = parsed.options.get("limit") orelse "20";
    const limit = try requireNumeric(limit_raw);
    const since = parsed.options.get("since");

    const sql = try buildListSql(allocator, limit, since);
    defer allocator.free(sql);

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print("{s}\n", .{sql});
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlCommand(allocator, db_url, sql);
}

fn runWeekly(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const week_start = parsed.options.get("week-start") orelse "date_trunc('week', now())";

    const sql = try buildWeeklySql(allocator, week_start);
    defer allocator.free(sql);

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print("{s}\n", .{sql});
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlCommand(allocator, db_url, sql);
}

fn runFollowups(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const since = parsed.options.get("since") orelse "current_date";
    const days_raw = parsed.options.get("days") orelse "14";
    const days = try requireNumeric(days_raw);

    const sql = try buildFollowupsSql(allocator, since, days);
    defer allocator.free(sql);

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print("{s}\n", .{sql});
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlCommand(allocator, db_url, sql);
}

fn runStaffSummary(allocator: std.mem.Allocator, parsed: *ParsedArgs) !void {
    const until = parsed.options.get("until") orelse "current_date";
    const days_raw = parsed.options.get("days") orelse "30";
    const days = try requireNumeric(days_raw);
    const limit_raw = parsed.options.get("limit") orelse "10";
    const limit = try requireNumeric(limit_raw);

    const sql = try buildStaffSummarySql(allocator, until, days, limit);
    defer allocator.free(sql);

    if (parsed.dry_run) {
        try std.io.getStdOut().writer().print("{s}\n", .{sql});
        return;
    }

    const db_url = try getDbUrl(allocator);
    defer allocator.free(db_url);

    try runPsqlCommand(allocator, db_url, sql);
}

fn getDbUrl(allocator: std.mem.Allocator) ![]u8 {
    return std.process.getEnvVarOwned(allocator, "GS_TOUCHPOINT_DB_URL");
}

fn runPsqlFile(allocator: std.mem.Allocator, db_url: []const u8, path: []const u8) !void {
    const argv = [_][]const u8{ "psql", db_url, "-v", "ON_ERROR_STOP=1", "-f", path };
    const result = try std.process.Child.run(.{ .allocator = allocator, .argv = &argv });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try handleResult(result);
}

fn runPsqlCommand(allocator: std.mem.Allocator, db_url: []const u8, sql: []const u8) !void {
    const argv = [_][]const u8{ "psql", db_url, "-v", "ON_ERROR_STOP=1", "-Atc", sql };
    const result = try std.process.Child.run(.{ .allocator = allocator, .argv = &argv });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try handleResult(result);

    if (result.stdout.len > 0) {
        try std.io.getStdOut().writer().print("{s}", .{result.stdout});
    }
}

fn handleResult(result: std.process.Child.RunResult) !void {
    switch (result.term) {
        .Exited => |code| if (code != 0) {
            try std.io.getStdErr().writer().print("psql failed ({d}): {s}\n", .{ code, result.stderr });
            return error.PsqlFailed;
        },
        else => {
            try std.io.getStdErr().writer().print("psql terminated: {s}\n", .{result.stderr});
            return error.PsqlFailed;
        },
    }
}

fn buildInsertSql(
    allocator: std.mem.Allocator,
    scholar: []const u8,
    scholar_id: ?[]const u8,
    channel: []const u8,
    staff: []const u8,
    notes: ?[]const u8,
    follow_up: ?[]const u8,
    occurred: ?[]const u8,
) ![]u8 {
    const scholar_literal = try sqlStringLiteral(allocator, scholar);
    defer allocator.free(scholar_literal);

    const scholar_id_literal = try sqlOptionalLiteral(allocator, scholar_id);
    defer allocator.free(scholar_id_literal);

    const channel_literal = try sqlStringLiteral(allocator, channel);
    defer allocator.free(channel_literal);

    const staff_literal = try sqlStringLiteral(allocator, staff);
    defer allocator.free(staff_literal);

    const notes_literal = try sqlOptionalLiteral(allocator, notes);
    defer allocator.free(notes_literal);

    const follow_up_literal = try sqlOptionalLiteral(allocator, follow_up);
    defer allocator.free(follow_up_literal);

    const occurred_literal = try sqlTimestampLiteral(allocator, occurred);
    defer allocator.free(occurred_literal);

    return std.fmt.allocPrint(
        allocator,
        "INSERT INTO touchpoint_ledger.touchpoints (scholar_name, scholar_identifier, channel, staff_name, notes, follow_up_date, occurred_at) VALUES ({s}, {s}, {s}, {s}, {s}, {s}, {s}) RETURNING id;",
        .{ scholar_literal, scholar_id_literal, channel_literal, staff_literal, notes_literal, follow_up_literal, occurred_literal },
    );
}

fn buildListSql(allocator: std.mem.Allocator, limit: []const u8, since: ?[]const u8) ![]u8 {
    if (since) |since_value| {
        const since_literal = try sqlStringLiteral(allocator, since_value);
        defer allocator.free(since_literal);

        return std.fmt.allocPrint(
            allocator,
            "SELECT id, scholar_name, channel, staff_name, occurred_at::date, follow_up_date FROM touchpoint_ledger.touchpoints WHERE occurred_at >= {s}::timestamptz ORDER BY occurred_at DESC LIMIT {s};",
            .{ since_literal, limit },
        );
    }

    return std.fmt.allocPrint(
        allocator,
        "SELECT id, scholar_name, channel, staff_name, occurred_at::date, follow_up_date FROM touchpoint_ledger.touchpoints ORDER BY occurred_at DESC LIMIT {s};",
        .{limit},
    );
}

fn buildWeeklySql(allocator: std.mem.Allocator, week_start: []const u8) ![]u8 {
    if (std.mem.eql(u8, week_start, "date_trunc('week', now())")) {
        return std.fmt.allocPrint(
            allocator,
            "WITH window AS (SELECT * FROM touchpoint_ledger.touchpoints WHERE occurred_at >= date_trunc('week', now()) AND occurred_at < date_trunc('week', now()) + interval '7 days') SELECT channel, count(*) AS touches, count(*) FILTER (WHERE follow_up_date IS NOT NULL AND follow_up_date <= date_trunc('week', now()) + interval '7 days') AS follow_ups_due FROM window GROUP BY channel ORDER BY touches DESC;",
            .{},
        );
    }

    const week_start_literal = try sqlStringLiteral(allocator, week_start);
    defer allocator.free(week_start_literal);

    return std.fmt.allocPrint(
        allocator,
        "WITH window AS (SELECT * FROM touchpoint_ledger.touchpoints WHERE occurred_at >= {s}::date AND occurred_at < {s}::date + interval '7 days') SELECT channel, count(*) AS touches, count(*) FILTER (WHERE follow_up_date IS NOT NULL AND follow_up_date <= {s}::date + interval '7 days') AS follow_ups_due FROM window GROUP BY channel ORDER BY touches DESC;",
        .{ week_start_literal, week_start_literal, week_start_literal },
    );
}

fn buildFollowupsSql(allocator: std.mem.Allocator, since: []const u8, days: []const u8) ![]u8 {
    const since_expr = try sqlDateExpression(allocator, since);
    defer allocator.free(since_expr);

    return std.fmt.allocPrint(
        allocator,
        "SELECT id, scholar_name, channel, staff_name, occurred_at::date, follow_up_date FROM touchpoint_ledger.touchpoints WHERE follow_up_date IS NOT NULL AND follow_up_date >= {s} AND follow_up_date <= {s} + interval '{s} days' ORDER BY follow_up_date ASC, occurred_at DESC;",
        .{ since_expr, since_expr, days },
    );
}

fn buildStaffSummarySql(allocator: std.mem.Allocator, until: []const u8, days: []const u8, limit: []const u8) ![]u8 {
    const until_expr = try sqlDateExpression(allocator, until);
    defer allocator.free(until_expr);

    return std.fmt.allocPrint(
        allocator,
        "WITH window AS (SELECT * FROM touchpoint_ledger.touchpoints WHERE occurred_at::date >= {s} - interval '{s} days' AND occurred_at::date <= {s}) SELECT staff_name, count(*) AS touches, count(*) FILTER (WHERE follow_up_date IS NOT NULL AND follow_up_date <= {s}) AS follow_ups_due, max(occurred_at)::date AS last_touch FROM window GROUP BY staff_name ORDER BY touches DESC, last_touch DESC LIMIT {s};",
        .{ until_expr, days, until_expr, until_expr, limit },
    );
}

fn sqlStringLiteral(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    var buffer = std.ArrayList(u8).empty;
    errdefer buffer.deinit(allocator);

    try buffer.append(allocator, '\'');
    for (value) |ch| {
        if (ch == '\'') {
            try buffer.appendSlice(allocator, "''");
        } else {
            try buffer.append(allocator, ch);
        }
    }
    try buffer.append(allocator, '\'');

    return buffer.toOwnedSlice(allocator);
}

fn sqlOptionalLiteral(allocator: std.mem.Allocator, value: ?[]const u8) ![]u8 {
    if (value) |actual| return sqlStringLiteral(allocator, actual);
    return allocator.dupe(u8, "NULL");
}

fn sqlTimestampLiteral(allocator: std.mem.Allocator, value: ?[]const u8) ![]u8 {
    if (value) |actual| {
        if (std.mem.eql(u8, actual, "now")) {
            return allocator.dupe(u8, "now()");
        }
        const literal = try sqlStringLiteral(allocator, actual);
        defer allocator.free(literal);
        return std.fmt.allocPrint(allocator, "{s}::timestamptz", .{literal});
    }
    return allocator.dupe(u8, "now()" );
}

fn sqlDateExpression(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    if (std.mem.eql(u8, value, "current_date")) {
        return allocator.dupe(u8, "current_date");
    }
    const literal = try sqlStringLiteral(allocator, value);
    defer allocator.free(literal);
    return std.fmt.allocPrint(allocator, "{s}::date", .{literal});
}

fn requireNumeric(value: []const u8) ![]const u8 {
    if (value.len == 0) return error.InvalidNumber;
    for (value) |ch| {
        if (ch < '0' or ch > '9') return error.InvalidNumber;
    }
    return value;
}

fn expectEqualString(expected: []const u8, actual: []const u8) !void {
    try std.testing.expectEqualStrings(expected, actual);
}

test "sqlStringLiteral escapes single quotes" {
    const allocator = std.testing.allocator;
    const out = try sqlStringLiteral(allocator, "O'Neil");
    defer allocator.free(out);

    try expectEqualString("'O''Neil'", out);
}

test "buildInsertSql uses nulls and now" {
    const allocator = std.testing.allocator;
    const out = try buildInsertSql(
        allocator,
        "Scholar Name",
        null,
        "email",
        "Staffer",
        null,
        null,
        null,
    );
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "NULL") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "now()") != null);
}

test "buildFollowupsSql uses current_date when requested" {
    const allocator = std.testing.allocator;
    const out = try buildFollowupsSql(allocator, "current_date", "10");
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "current_date") != null);
}

test "buildStaffSummarySql uses interval and limit" {
    const allocator = std.testing.allocator;
    const out = try buildStaffSummarySql(allocator, "current_date", "30", "5");
    defer allocator.free(out);

    try std.testing.expect(std.mem.indexOf(u8, out, "interval '30 days'") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "LIMIT 5") != null);
}

test "requireNumeric rejects non-digits" {
    try std.testing.expectError(error.InvalidNumber, requireNumeric("10d"));
}
