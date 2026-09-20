import Foundation

/// Reads bounded pages while retaining raw segments for only the current session.
/// Memory is proportional to the largest session plus one selected summary per day,
/// rather than every raw sleep record. Nothing is reconciled until the complete scan
/// succeeds; cancellation, decode/query failure, or a concurrent write preserves the
/// prior derived rows. A subsequent collection can safely retry the entire calculation.
enum SleepDerivation {
    static let rawType = "HKCategoryTypeIdentifierSleepAnalysis"
    static let derivedType = "boaz.sleep.deep_minutes"

    static func refresh(database: BoazLocalDatabase, now: Date = Date(), calendar: Calendar = .current) async throws -> Int {
        var (accumulator, version) = try await readHistory(database: database, now: now, calendar: calendar,
                                                         retainDailySummaries: true)
        let derived = accumulator.finish()
        try Task.checkCancellation()
        return try await database.reconcileDerivedSleep(derived, expectedVersion: version)
    }

    /// The dashboard uses complete sessions from the same stable, paged history as
    /// derivation. No age or row cap can truncate a session or hide the last night.
    static func latestCompletedSession(database: BoazLocalDatabase, now: Date = Date(),
                                       calendar: Calendar = .current) async throws -> SleepSession? {
        var (accumulator, version) = try await readHistory(database: database, now: now, calendar: calendar,
                                                         retainDailySummaries: false)
        let session = accumulator.finishLatestCompletedSession()
        try Task.checkCancellation()
        // Validate once more after final session calculation, which can be lengthy.
        // A one-row page performs the existing before/after SQLite version checks.
        _ = try await database.historyPage(typeIdentifier: rawType, limit: 1, expectedVersion: version)
        try Task.checkCancellation()
        return session
    }

    private static func readHistory(database: BoazLocalDatabase, now: Date, calendar: Calendar,
                                    retainDailySummaries: Bool) async throws -> (SleepHistoryAccumulator, HealthHistoryVersion) {
        var accumulator = SleepHistoryAccumulator(now: now, calendar: calendar,
                                                  retainDailySummaries: retainDailySummaries)
        var cursor: HealthHistoryCursor?
        var version: HealthHistoryVersion?
        repeat {
            try Task.checkCancellation()
            let page = try await database.historyPage(typeIdentifier: rawType, after: cursor, expectedVersion: version)
            version = page.version
            try accumulator.append(page.events)
            cursor = page.nextCursor
            await Task.yield()
        } while cursor != nil
        try Task.checkCancellation()
        guard let version else { throw HealthHistoryError.changedDuringRead }
        return (accumulator, version)
    }
}

struct SleepHistoryAccumulator {
    private let now: Date
    private let calendar: Calendar
    private let retainDailySummaries: Bool
    private var current: [SleepSegment] = []
    private var groupEnd = Date.distantPast
    private var previousStart: Date?
    private var selected: [String: SleepSession] = [:]
    private var latestCompleted: SleepSession?

    init(now: Date, calendar: Calendar, retainDailySummaries: Bool = true) {
        self.now = now
        self.calendar = calendar
        self.retainDailySummaries = retainDailySummaries
    }

    mutating func append(_ events: [HealthEvent]) throws {
        for segment in SleepAnalyzer.segments(from: events) {
            guard segment.start < segment.end, segment.end.timeIntervalSince(segment.start) <= 24 * 3600 else { continue }
            if let previousStart, segment.start < previousStart { throw HealthHistoryError.unorderedHistory }
            previousStart = segment.start
            if !current.isEmpty, segment.start.timeIntervalSince(groupEnd) > SleepAnalyzer.sessionGap {
                completeSession()
            }
            current.append(segment)
            groupEnd = max(groupEnd, segment.end)
        }
    }

    mutating func finish() -> [HealthEvent] {
        completeSession()
        return selected.keys.sorted().compactMap { day in
            guard let session = selected[day] else { return nil }
            return HealthEvent(
                eventID: "sleep-day:\(day)", revision: 0, operation: "upsert", kind: "quantity",
                type: SleepDerivation.derivedType, sourceBundleID: "boazapp", sourceName: "Boaz SleepAnalyzer",
                startUTC: session.start, endUTC: session.end, value: session.stages.deepSeconds / 60,
                unit: "min", metadata: ["derivation_version": "1", "sleep_day": day, "time_zone": calendar.timeZone.identifier]
            )
        }
    }

    mutating func finishLatestCompletedSession() -> SleepSession? {
        completeSession()
        return latestCompleted
    }

    private mutating func completeSession() {
        defer {
            current.removeAll(keepingCapacity: true)
            groupEnd = .distantPast
        }
        guard !current.isEmpty, let session = SleepAnalyzer.sessions(from: current, calendar: calendar).first else { return }
        latestCompleted = SleepAnalyzer.latestCompletedSession(from: [latestCompleted, session].compactMap { $0 }, now: now)
        guard retainDailySummaries, !session.isNap, session.asleepSeconds > 0,
              session.end <= now.addingTimeInterval(-600) else { return }
        let parts = calendar.dateComponents([.year, .month, .day], from: session.end)
        let day = String(format: "%04d-%02d-%02d", parts.year ?? 0, parts.month ?? 0, parts.day ?? 0)
        if let prior = selected[day] {
            guard session.asleepSeconds > prior.asleepSeconds
                    || (session.asleepSeconds == prior.asleepSeconds && session.end > prior.end) else { return }
        }
        selected[day] = session
    }
}
