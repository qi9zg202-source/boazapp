import Foundation
import SQLite3
import XCTest
@testable import boazapp

final class SleepHistoryTests: XCTestCase {
    private let origin = Date(timeIntervalSince1970: 1_577_836_800) // 2020-01-01 00:00 UTC

    private var calendar: Calendar {
        var value = Calendar(identifier: .gregorian)
        value.timeZone = TimeZone(secondsFromGMT: 0)!
        return value
    }

    private var schemaURL: URL? {
        Bundle(for: SleepHistoryTests.self).url(forResource: "Schema", withExtension: "sql")
    }

    private func database() throws -> (BoazLocalDatabase, URL) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let url = directory.appendingPathComponent("health.sqlite")
        return (try BoazLocalDatabase(url: url, schemaURL: schemaURL), url)
    }

    private func sample(_ id: String, start: Date, end: Date, stage: SleepStage = .asleepDeep) -> HealthEvent {
        HealthEvent(eventID: id, revision: 0, operation: "upsert", kind: "category",
                    type: SleepDerivation.rawType, sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                    startUTC: start, endUTC: end, value: Double(stage.rawValue), unit: "category-value", metadata: [:])
    }

    private func oldDerived(_ id: String = "sleep-day:1999-01-01") -> HealthEvent {
        HealthEvent(eventID: id, revision: 0, operation: "upsert", kind: "quantity",
                    type: SleepDerivation.derivedType, sourceBundleID: "boazapp", sourceName: "Boaz SleepAnalyzer",
                    startUTC: origin.addingTimeInterval(-100_000), endUTC: origin.addingTimeInterval(-90_000),
                    value: 5, unit: "min", metadata: ["derivation_version": "1"])
    }

    /// Seeds a real SQLite history efficiently. This tests the production history reader
    /// and derivation against 100,001 persisted rows; it does not claim HealthKit ingestion.
    private func seedHistory(at url: URL, count: Int, perNight: Int = 137, corruptLast: Bool = false) throws {
        var connection: OpaquePointer?
        guard sqlite3_open(url.path, &connection) == SQLITE_OK, let connection else {
            throw LocalDatabaseError.query("Could not open synthetic history fixture")
        }
        defer { sqlite3_close(connection) }
        guard sqlite3_exec(connection, "BEGIN IMMEDIATE", nil, nil, nil) == SQLITE_OK else {
            throw LocalDatabaseError.query("Could not start synthetic history fixture")
        }
        var statement: OpaquePointer?
        let sql = """
        INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,start_utc,end_utc,value,unit,payload,content_hash,updated_at)
        VALUES(?,1,'upsert','category',?,?,?,4,'category-value',?,'synthetic-fixture','2020-01-01T00:00:00Z')
        """
        guard sqlite3_prepare_v2(connection, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw LocalDatabaseError.query("Could not prepare synthetic history fixture")
        }
        defer { sqlite3_finalize(statement) }
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        let formatter = ISO8601DateFormatter()
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        for index in 0..<count {
            let start = origin.addingTimeInterval(Double(index / perNight) * 86_400 + 22 * 3600)
            let event = sample(String(format: "sample-%06d", index), start: start, end: start.addingTimeInterval(8 * 3600))
            let payload = corruptLast && index == count - 1 ? Data("{malformed".utf8) : try encoder.encode(event)
            for (offset, text) in [event.eventID, event.type, formatter.string(from: start), formatter.string(from: event.endUTC!)].enumerated() {
                let code = text.withCString { sqlite3_bind_text(statement, Int32(offset + 1), $0, -1, transient) }
                guard code == SQLITE_OK else { throw LocalDatabaseError.query("Fixture text bind failed") }
            }
            let code = payload.withUnsafeBytes { sqlite3_bind_blob(statement, 5, $0.baseAddress, Int32($0.count), transient) }
            guard code == SQLITE_OK, sqlite3_step(statement) == SQLITE_DONE else {
                throw LocalDatabaseError.query("Fixture insert failed")
            }
            sqlite3_reset(statement)
            sqlite3_clear_bindings(statement)
        }
        guard sqlite3_exec(connection, "COMMIT", nil, nil, nil) == SQLITE_OK else {
            throw LocalDatabaseError.query("Fixture commit failed")
        }
    }

    func testMoreThan100000PersistedSamplesDeriveEveryNightAndRemoveOnlyStaleDay() async throws {
        let (ledger, url) = try database()
        let count = 100_001
        try seedHistory(at: url, count: count)
        _ = try await ledger.apply(events: [oldDerived()])
        let before = try await ledger.counts()
        XCTAssertEqual(before.records, count + 1)

        let changed = try await SleepDerivation.refresh(database: ledger, now: origin.addingTimeInterval(1_000 * 86_400), calendar: calendar)
        let expectedNights = (count + 136) / 137
        XCTAssertEqual(changed, expectedNights + 1)
        let derived = try await ledger.events(typeIdentifier: SleepDerivation.derivedType, since: .distantPast)
        XCTAssertEqual(derived.count, expectedNights)
        XCTAssertTrue(derived.allSatisfy { $0.value == 480 && $0.unit == "min" })
        XCTAssertEqual(derived.map(\.startUTC).compactMap { $0 }.min(), origin.addingTimeInterval(22 * 3600))
        XCTAssertEqual(derived.map(\.startUTC).compactMap { $0 }.max(), origin.addingTimeInterval(Double(expectedNights - 1) * 86_400 + 22 * 3600))
        XCTAssertFalse(derived.contains { $0.eventID == "sleep-day:1999-01-01" })
        let unchanged = try await SleepDerivation.refresh(database: ledger, now: origin.addingTimeInterval(1_000 * 86_400), calendar: calendar)
        XCTAssertEqual(unchanged, 0)

        // Remove every raw sample from the first night. Only that derived day may disappear.
        let deletions = (0..<137).map { index in
            HealthEvent(eventID: String(format: "sample-%06d", index), revision: 0, operation: "delete", kind: "category",
                        type: SleepDerivation.rawType, sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                        value: nil, unit: nil, metadata: [:])
        }
        _ = try await ledger.apply(events: deletions)
        let removed = try await SleepDerivation.refresh(database: ledger, now: origin.addingTimeInterval(1_000 * 86_400), calendar: calendar)
        XCTAssertEqual(removed, 1)
        let remaining = try await ledger.events(typeIdentifier: SleepDerivation.derivedType, since: .distantPast)
        XCTAssertEqual(remaining.count, expectedNights - 1)
        XCTAssertFalse(remaining.contains { $0.eventID == "sleep-day:2020-01-02" })

        // Re-key days in another timezone using independent calendar expectations.
        var shiftedCalendar = calendar
        shiftedCalendar.timeZone = TimeZone(identifier: "Pacific/Honolulu")!
        _ = try await SleepDerivation.refresh(database: ledger, now: origin.addingTimeInterval(1_000 * 86_400), calendar: shiftedCalendar)
        let shifted = try await ledger.events(typeIdentifier: SleepDerivation.derivedType, since: .distantPast)
        let expectedIDs = Set((1..<expectedNights).map { night -> String in
            let end = origin.addingTimeInterval(Double(night) * 86_400 + 30 * 3600)
            let date = shiftedCalendar.dateComponents([.year, .month, .day], from: end)
            return String(format: "sleep-day:%04d-%02d-%02d", date.year!, date.month!, date.day!)
        })
        XCTAssertEqual(Set(shifted.map(\.eventID)), expectedIDs)
        XCTAssertTrue(shifted.allSatisfy { $0.value == 480 && $0.metadata["time_zone"] == "Pacific/Honolulu" && $0.metadata["derivation_version"] == "1" })
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testHistoryKeysetDoesNotSkipEqualTimestampsOrExactBoundary() async throws {
        let (ledger, url) = try database()
        try seedHistory(at: url, count: 1_000, perNight: 1_000)
        var cursor: HealthHistoryCursor?
        var version: HealthHistoryVersion?
        var identifiers: [String] = []
        var pages = 0
        repeat {
            let page = try await ledger.historyPage(typeIdentifier: SleepDerivation.rawType, after: cursor, expectedVersion: version)
            pages += 1
            XCTAssertLessThanOrEqual(page.events.count, 500)
            version = page.version
            identifiers += page.events.map(\.eventID)
            cursor = page.nextCursor
        } while cursor != nil
        XCTAssertEqual(pages, 3)
        XCTAssertEqual(identifiers, (0..<1_000).map { String(format: "sample-%06d", $0) })
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLatestCompletedReadsOlderThanSevenDaysAndMoreThan10000SamplesInOneSession() async throws {
        let (ledger, url) = try database()
        try seedHistory(at: url, count: 10_001, perNight: 10_001)
        let latest = try await SleepDerivation.latestCompletedSession(
            database: ledger, now: origin.addingTimeInterval(20 * 86_400), calendar: calendar)
        let session = try XCTUnwrap(latest)
        XCTAssertEqual(session.start, origin.addingTimeInterval(22 * 3600))
        XCTAssertEqual(session.end, origin.addingTimeInterval(30 * 3600))
        XCTAssertEqual(session.sampleCount, 10_001)
        XCTAssertEqual(session.asleepSeconds, 8 * 3600)
        XCTAssertEqual(session.stages.deepSeconds, 8 * 3600)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.records, 10_001)
        XCTAssertEqual(counts.pending, 0, "Reading the sleep card must not create upload work")
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLatestCompletedPrefersLastMainSleepOverNapAndRecentUnsettledSession() async throws {
        let (ledger, url) = try database()
        let firstStart = origin.addingTimeInterval(22 * 3600)
        let lastStart = origin.addingTimeInterval(46 * 3600)
        let events = [
            sample("earlier-night", start: firstStart, end: firstStart.addingTimeInterval(8 * 3600)),
            sample("last-night", start: lastStart, end: lastStart.addingTimeInterval(8 * 3600)),
            sample("afternoon-nap", start: origin.addingTimeInterval(61 * 3600), end: origin.addingTimeInterval(62 * 3600)),
            sample("unsettled-night", start: origin.addingTimeInterval(70 * 3600), end: origin.addingTimeInterval(78 * 3600))
        ]
        _ = try await ledger.apply(events: events)
        let now = origin.addingTimeInterval(78 * 3600 + 5 * 60)
        let latest = try await SleepDerivation.latestCompletedSession(database: ledger, now: now, calendar: calendar)
        XCTAssertEqual(latest?.start, lastStart)
        XCTAssertEqual(latest?.isNap, false)
        let reference = SleepAnalyzer.latestCompletedSession(
            from: SleepAnalyzer.sessions(from: SleepAnalyzer.segments(from: events), calendar: calendar), now: now)
        XCTAssertEqual(latest, reference)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLatestCompletedUsesMostRecentNapWhenNoMainSleepExists() async throws {
        let (ledger, url) = try database()
        let lastNapStart = origin.addingTimeInterval(37 * 3600)
        _ = try await ledger.apply(events: [
            sample("first-nap", start: origin.addingTimeInterval(13 * 3600), end: origin.addingTimeInterval(14 * 3600)),
            sample("last-nap", start: lastNapStart, end: lastNapStart.addingTimeInterval(3600))
        ])
        let latest = try await SleepDerivation.latestCompletedSession(
            database: ledger, now: origin.addingTimeInterval(40 * 3600), calendar: calendar)
        XCTAssertEqual(latest?.start, lastNapStart)
        XCTAssertEqual(latest?.isNap, true)
        XCTAssertEqual(latest?.asleepSeconds, 3600)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLatestCompletedDoesNotReturnPartialHistoryOnCancellationOrDecodeFailure() async throws {
        let (ledger, url) = try database()
        try seedHistory(at: url, count: 501, corruptLast: true)
        let selectedCalendar = calendar
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await SleepDerivation.latestCompletedSession(database: ledger, calendar: selectedCalendar)
        }
        do {
            _ = try await task.value
            XCTFail("Cancelled card calculation must not return a partial session")
        } catch is CancellationError {}
        do {
            _ = try await SleepDerivation.latestCompletedSession(database: ledger, now: origin.addingTimeInterval(20 * 86_400), calendar: calendar)
            XCTFail("A corrupt later page must not return an earlier partial session")
        } catch is DecodingError {}
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.records, 501)
        XCTAssertEqual(counts.pending, 0)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testSessionSpanningPagesKeepsOverlapRulesAndExcludesNap() throws {
        var events: [HealthEvent] = []
        for day in 0..<2 {
            let start = origin.addingTimeInterval(Double(day) * 86_400 + 22 * 3600)
            events += [
                sample("core-\(day)", start: start, end: start.addingTimeInterval(2 * 3600), stage: .asleepCore),
                sample("deep-\(day)", start: start.addingTimeInterval(3600), end: start.addingTimeInterval(3 * 3600)),
                sample("rem-\(day)", start: start.addingTimeInterval(3 * 3600), end: start.addingTimeInterval(8 * 3600), stage: .asleepREM)
            ]
        }
        events.append(sample("nap", start: origin.addingTimeInterval(37 * 3600), end: origin.addingTimeInterval(38 * 3600)))
        events.sort { $0.startUTC! < $1.startUTC! }
        var accumulator = SleepHistoryAccumulator(now: origin.addingTimeInterval(4 * 86_400), calendar: calendar)
        for index in stride(from: 0, to: events.count, by: 2) {
            try accumulator.append(Array(events[index..<min(index + 2, events.count)]))
        }
        let result = accumulator.finish()
        XCTAssertEqual(result.map(\.eventID), ["sleep-day:2020-01-02", "sleep-day:2020-01-03"])
        XCTAssertEqual(result.map(\.value), [120, 120])
    }

    func testConcurrentHistoryChangeCannotPrunePriorDerivedRows() async throws {
        let (ledger, url) = try database()
        _ = try await ledger.apply(events: [oldDerived()])
        let page = try await ledger.historyPage(typeIdentifier: SleepDerivation.rawType)
        let otherConnection = try BoazLocalDatabase(url: url, schemaURL: schemaURL)
        _ = try await otherConnection.apply(events: [sample("new", start: origin, end: origin.addingTimeInterval(8 * 3600))])
        do {
            _ = try await ledger.historyPage(typeIdentifier: SleepDerivation.rawType, limit: 1, expectedVersion: page.version)
            XCTFail("Dashboard history validation must reject a changed snapshot")
        } catch HealthHistoryError.changedDuringRead {}
        do {
            _ = try await ledger.reconcileDerivedSleep([], expectedVersion: page.version)
            XCTFail("A changed snapshot must not prune earlier derived days")
        } catch HealthHistoryError.changedDuringRead {}
        let retained = try await ledger.latestEvent(typeIdentifier: SleepDerivation.derivedType)
        XCTAssertEqual(retained?.eventID, "sleep-day:1999-01-01")
        try await otherConnection.closeForTesting()
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLatePageDecodeFailureKeepsPriorDerivedRows() async throws {
        let (ledger, url) = try database()
        try seedHistory(at: url, count: 501, corruptLast: true)
        _ = try await ledger.apply(events: [oldDerived()])
        do {
            _ = try await SleepDerivation.refresh(database: ledger, now: origin.addingTimeInterval(10 * 86_400), calendar: calendar)
            XCTFail("The corrupt second page must abort reconciliation")
        } catch is DecodingError {}
        let retained = try await ledger.latestEvent(typeIdentifier: SleepDerivation.derivedType)
        XCTAssertEqual(retained?.eventID, "sleep-day:1999-01-01")
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testCancellationKeepsPriorDerivedRows() async throws {
        let (ledger, url) = try database()
        _ = try await ledger.apply(events: [oldDerived()])
        let selectedCalendar = calendar
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await SleepDerivation.refresh(database: ledger, calendar: selectedCalendar)
        }
        do {
            _ = try await task.value
            XCTFail("Cancelled history calculation must abort")
        } catch is CancellationError {}
        let retained = try await ledger.latestEvent(typeIdentifier: SleepDerivation.derivedType)
        XCTAssertEqual(retained?.eventID, "sleep-day:1999-01-01")
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }
}
