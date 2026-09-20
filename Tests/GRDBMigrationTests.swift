import CryptoKit
import Foundation
import SQLite3
import XCTest
@testable import boazapp

/// Synthetic fixtures only. SQLite3 is used here solely to emulate a legacy
/// database or a separate external writer; the app ledger uses GRDB.
final class GRDBMigrationTests: XCTestCase {
    private final class ProtectionGate: @unchecked Sendable {
        private let lock = NSLock()
        private var checks = 0

        func verify(_ url: URL) throws {
            lock.lock()
            checks += 1
            let shouldFail = checks == 2
            lock.unlock()
            if shouldFail { throw LocalDatabaseError.storageProtectionUnverified("Synthetic sidecar failure") }
        }
    }

    private func fixtureURL() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("grdb-migration-\(UUID().uuidString)", isDirectory: true)
            .appendingPathComponent("health.sqlite")
    }

    private func event(_ id: String, type: String = SleepDerivation.rawType,
                       value: Double = 72) -> HealthEvent {
        HealthEvent(eventID: id, revision: 0, operation: "upsert", kind: "quantity",
                    type: type, sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                    startUTC: Date(timeIntervalSince1970: 1_700_000_000),
                    endUTC: Date(timeIntervalSince1970: 1_700_000_001),
                    value: value, unit: "count/min", metadata: [:])
    }

    private func externalSQL(at url: URL, _ sql: String) throws {
        var pointer: OpaquePointer?
        guard sqlite3_open(url.path, &pointer) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query("Could not open synthetic external writer.")
        }
        defer { sqlite3_close(pointer) }
        var error: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(pointer, sql, nil, nil, &error) == SQLITE_OK else {
            let detail = error.map { String(cString: $0) } ?? "Synthetic SQL failed."
            if let error { sqlite3_free(error) }
            throw LocalDatabaseError.query(detail)
        }
    }

    private func externalInt(at url: URL, _ sql: String) throws -> Int {
        var pointer: OpaquePointer?
        guard sqlite3_open(url.path, &pointer) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query("Could not open synthetic reader.")
        }
        defer { sqlite3_close(pointer) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(pointer, sql, -1, &statement, nil) == SQLITE_OK,
              let statement else { throw LocalDatabaseError.query("Synthetic query failed.") }
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else {
            throw LocalDatabaseError.query("Synthetic query returned no row.")
        }
        return Int(sqlite3_column_int64(statement, 0))
    }

    private func externalBlob(at url: URL, _ sql: String) throws -> Data {
        var pointer: OpaquePointer?
        guard sqlite3_open(url.path, &pointer) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query("Could not open synthetic reader.")
        }
        defer { sqlite3_close(pointer) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(pointer, sql, -1, &statement, nil) == SQLITE_OK,
              let statement else { throw LocalDatabaseError.query("Synthetic blob query failed.") }
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW,
              let bytes = sqlite3_column_blob(statement, 0) else {
            throw LocalDatabaseError.query("Synthetic blob query returned no data.")
        }
        return Data(bytes: bytes, count: Int(sqlite3_column_bytes(statement, 0)))
    }

    private func externalText(at url: URL, _ sql: String) throws -> String {
        var pointer: OpaquePointer?
        guard sqlite3_open(url.path, &pointer) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query("Could not open synthetic reader.")
        }
        defer { sqlite3_close(pointer) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(pointer, sql, -1, &statement, nil) == SQLITE_OK,
              let statement else { throw LocalDatabaseError.query("Synthetic text query failed.") }
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW,
              let text = sqlite3_column_text(statement, 0) else {
            throw LocalDatabaseError.query("Synthetic text query returned no data.")
        }
        return String(cString: text)
    }

    private func externalPlan(at url: URL, _ sql: String) throws -> [String] {
        var pointer: OpaquePointer?
        guard sqlite3_open(url.path, &pointer) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query("Could not open synthetic query planner.")
        }
        defer { sqlite3_close(pointer) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(pointer, "EXPLAIN QUERY PLAN " + sql, -1, &statement, nil) == SQLITE_OK,
              let statement else { throw LocalDatabaseError.query("Synthetic plan query failed.") }
        defer { sqlite3_finalize(statement) }
        var plans: [String] = []
        while sqlite3_step(statement) == SQLITE_ROW {
            guard let text = sqlite3_column_text(statement, 3) else { continue }
            plans.append(String(cString: text))
        }
        return plans
    }

    private func hex(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }

    private let downgradeToV1 = """
        DROP TRIGGER health_events_clock_insert;
        DROP TRIGGER health_events_clock_update;
        DROP TRIGGER health_events_clock_delete;
        DROP TABLE local_change_clock;
        PRAGMA user_version=1;
        """

    func testWriterUsesWALFullForeignKeysAndV2() async throws {
        let url = fixtureURL()
        let ledger = try BoazLocalDatabase(url: url)
        let settings = try await ledger.configurationEvidence()
        XCTAssertEqual(settings.journalMode.lowercased(), "wal")
        XCTAssertEqual(settings.synchronous, 2)
        XCTAssertEqual(settings.foreignKeys, 1)
        XCTAssertEqual(settings.userVersion, 2)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testLegacyV1MigratesInPlaceWithoutLosingEventOrAnchor() async throws {
        let url = fixtureURL()
        let first = try BoazLocalDatabase(url: url)
        let anchor = Data([1, 2, 3, 4])
        _ = try await first.apply(events: [event("legacy-event")], anchor: anchor,
                                  typeIdentifier: SleepDerivation.rawType)
        try await first.closeForTesting()
        try externalSQL(at: url, downgradeToV1)

        let migrated = try BoazLocalDatabase(url: url)
        let settings = try await migrated.configurationEvidence()
        let savedAnchor = try await migrated.anchor(for: SleepDerivation.rawType)
        let counts = try await migrated.counts()
        XCTAssertEqual(settings.userVersion, 2)
        XCTAssertEqual(savedAnchor, anchor)
        XCTAssertEqual(counts.records, 1)
        XCTAssertEqual(try externalInt(at: url, "SELECT COUNT(*) FROM local_change_clock WHERE id=1"), 1)
        try await migrated.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testUnversionedLegacyLayoutMigratesWithoutDroppingRows() async throws {
        let url = fixtureURL()
        let first = try BoazLocalDatabase(url: url)
        _ = try await first.apply(events: [event("unversioned-event")])
        try await first.closeForTesting()
        try externalSQL(at: url, downgradeToV1 + "\nPRAGMA user_version=0;")
        let migrated = try BoazLocalDatabase(url: url)
        let settings = try await migrated.configurationEvidence()
        let counts = try await migrated.counts()
        XCTAssertEqual(settings.userVersion, 2)
        XCTAssertEqual(counts.records, 1)
        try await migrated.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testFrozenHistoricalV1FixturePreservesRowsAnchorReceiptAndRetryBytes() async throws {
        let url = fixtureURL()
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        let fixtureBundle = Bundle(for: GRDBMigrationTests.self)
        let schemaURL = try XCTUnwrap(fixtureBundle.url(forResource: "SchemaV1", withExtension: "sql"))
        let schema = try String(contentsOf: schemaURL, encoding: .utf8)
        try externalSQL(at: url, schema + "\nPRAGMA user_version=1;")

        let anchor = Data([0x01, 0x02, 0x03])
        let savedEvent = event("legacy-cloud-saved")
        let pendingEvent = event("legacy-retry")
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let savedPayload = try encoder.encode(savedEvent)
        let pendingPayload = try encoder.encode(pendingEvent)
        let savedBody = try JSONSerialization.data(withJSONObject: [
            "batch_id": "legacy-saved-batch", "device_id": "synthetic-device", "schema_version": 1,
            "events": [["event_id": savedEvent.eventID]]
        ], options: [.sortedKeys])
        let pendingBody = try JSONSerialization.data(withJSONObject: [
            "batch_id": "legacy-retry-batch", "device_id": "synthetic-device", "schema_version": 1,
            "events": [["event_id": pendingEvent.eventID]]
        ], options: [.sortedKeys])
        let savedHash = SHA256.hash(data: savedBody).map { String(format: "%02x", $0) }.joined()
        let pendingHash = SHA256.hash(data: pendingBody).map { String(format: "%02x", $0) }.joined()
        let receipt = TokyoReceipt(batchID: "legacy-saved-batch", status: "cloud_saved", acceptedEvents: 1,
                                   receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                   contentHash: savedHash, commitSequence: 1)
        let receiptBytes = try encoder.encode(receipt)
        try externalSQL(at: url, """
            INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,start_utc,end_utc,value,unit,payload,content_hash,updated_at)
            VALUES('legacy-cloud-saved',1,'upsert','quantity','\(SleepDerivation.rawType)','2023-11-14T22:13:20Z','2023-11-14T22:13:21Z',72,'count/min',X'\(hex(savedPayload))','legacy-hash-a','2023-11-14T22:13:20Z');
            INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,start_utc,end_utc,value,unit,payload,content_hash,updated_at)
            VALUES('legacy-retry',1,'upsert','quantity','\(SleepDerivation.rawType)','2023-11-14T22:13:20Z','2023-11-14T22:13:21Z',72,'count/min',X'\(hex(pendingPayload))','legacy-hash-b','2023-11-14T22:13:20Z');
            INSERT INTO query_anchors(type_identifier,anchor,updated_at)
            VALUES('\(SleepDerivation.rawType)',X'\(hex(anchor))','2023-11-14T22:13:20Z');
            INSERT INTO upload_batches(batch_id,body,body_hash,state,receipt,created_at,updated_at)
            VALUES('legacy-saved-batch',X'\(hex(savedBody))','\(savedHash)','cloud_saved',X'\(hex(receiptBytes))','2023-11-14T22:13:20Z','2023-11-14T22:13:20Z');
            INSERT INTO upload_outbox(event_id,revision,state,batch_id,updated_at)
            VALUES('legacy-cloud-saved',1,'cloud_saved','legacy-saved-batch','2023-11-14T22:13:20Z');
            INSERT INTO upload_batches(batch_id,body,body_hash,state,attempts,next_attempt_at,last_error,created_at,updated_at)
            VALUES('legacy-retry-batch',X'\(hex(pendingBody))','\(pendingHash)','pending',3,'2000-01-01T00:00:00Z','transient','2023-11-14T22:13:21Z','2023-11-14T22:13:21Z');
            INSERT INTO upload_outbox(event_id,revision,state,batch_id,attempts,next_attempt_at,last_error,updated_at)
            VALUES('legacy-retry',1,'sending','legacy-retry-batch',3,'2000-01-01T00:00:00Z','transient','2023-11-14T22:13:21Z');
            """)
        let originalReceipt = try externalBlob(at: url,
            "SELECT receipt FROM upload_batches WHERE batch_id='legacy-saved-batch'")

        let migrated = try BoazLocalDatabase(url: url)
        let settings = try await migrated.configurationEvidence()
        let counts = try await migrated.counts()
        let savedAnchor = try await migrated.anchor(for: SleepDerivation.rawType)
        let savedIDs = try await migrated.cloudSavedBatchIDs()
        let retry = try await migrated.prepareBatch(deviceID: "synthetic-device")
        XCTAssertEqual(settings.userVersion, 2)
        XCTAssertEqual(counts.records, 2)
        XCTAssertEqual(counts.pending, 1)
        XCTAssertEqual(counts.cloudSaved, 1)
        XCTAssertEqual(savedAnchor, anchor)
        XCTAssertEqual(savedIDs, ["legacy-saved-batch"])
        XCTAssertEqual(retry?.id, "legacy-retry-batch")
        XCTAssertEqual(retry?.body, pendingBody)
        XCTAssertEqual(retry?.contentHash, pendingHash)
        XCTAssertEqual(try externalBlob(at: url,
            "SELECT receipt FROM upload_batches WHERE batch_id='legacy-saved-batch'"), originalReceipt)
        XCTAssertEqual(try externalInt(at: url,
            "SELECT attempts FROM upload_batches WHERE batch_id='legacy-retry-batch'"), 3)
        let current = TokyoReceipt(batchID: "legacy-saved-batch", status: "metrics_current", acceptedEvents: 1,
                                   receivedAt: receipt.receivedAt, projectedAt: "2026-09-18T00:01:00Z",
                                   contentHash: savedHash, commitSequence: 1)
        try await migrated.markMetricsCurrent(batchID: "legacy-saved-batch", receipt: current)
        let after = try await migrated.counts()
        XCTAssertEqual(after.metricsCurrent, 1)
        try await migrated.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testFailedMigrationLeavesV1AndItsRecordsUntouched() async throws {
        let url = fixtureURL()
        let first = try BoazLocalDatabase(url: url)
        _ = try await first.apply(events: [event("legacy-retained")])
        try await first.closeForTesting()
        try externalSQL(at: url, downgradeToV1 + "\nCREATE TABLE local_change_clock(bad INTEGER);")
        XCTAssertThrowsError(try BoazLocalDatabase(url: url))
        XCTAssertEqual(try externalInt(at: url, "PRAGMA user_version"), 1)
        XCTAssertEqual(try externalInt(at: url, "SELECT COUNT(*) FROM health_events"), 1)
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    #if DEBUG
    func testInjectedFailureInsideV1MigrationRollsBackSchemaAnchorAndOutbox() async throws {
        let url = fixtureURL()
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        let fixtureBundle = Bundle(for: GRDBMigrationTests.self)
        let schemaURL = try XCTUnwrap(fixtureBundle.url(forResource: "SchemaV1", withExtension: "sql"))
        let schema = try String(contentsOf: schemaURL, encoding: .utf8)
        let payload = try JSONEncoder().encode(event("rollback-event"))
        let anchor = Data([0x10, 0x20, 0x30])
        try externalSQL(at: url, schema + """
            \nPRAGMA user_version=1;
            INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,payload,content_hash,updated_at)
            VALUES('rollback-event',1,'upsert','quantity','\(SleepDerivation.rawType)',X'\(hex(payload))','legacy','2023-11-14T22:13:20Z');
            INSERT INTO query_anchors(type_identifier,anchor,updated_at)
            VALUES('\(SleepDerivation.rawType)',X'\(hex(anchor))','2023-11-14T22:13:20Z');
            INSERT INTO upload_outbox(event_id,revision,state,attempts,next_attempt_at,last_error,updated_at)
            VALUES('rollback-event',1,'pending',2,'2000-01-01T00:00:00Z','previous retry','2023-11-14T22:13:20Z');
            """)
        let schemaQuery = """
            SELECT group_concat(name || ':' || sql,'|') FROM
              (SELECT name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name)
            """
        let schemaBefore = try externalText(at: url, schemaQuery)
        let payloadBefore = try externalBlob(at: url,
            "SELECT payload FROM health_events WHERE event_id='rollback-event'")
        do {
            _ = try BoazLocalDatabase(url: url, migrationFailureProbe: {
                throw LocalDatabaseError.query("Injected after clock DDL, before version stamp")
            })
            XCTFail("Injected migration failure should roll back the writer transaction.")
        } catch LocalDatabaseError.query {}
        XCTAssertEqual(try externalInt(at: url, "PRAGMA user_version"), 1)
        XCTAssertEqual(try externalText(at: url, schemaQuery), schemaBefore)
        XCTAssertEqual(try externalBlob(at: url,
            "SELECT payload FROM health_events WHERE event_id='rollback-event'"), payloadBefore)
        XCTAssertEqual(try externalBlob(at: url,
            "SELECT anchor FROM query_anchors WHERE type_identifier='\(SleepDerivation.rawType)'"), anchor)
        XCTAssertEqual(try externalInt(at: url,
            "SELECT attempts FROM upload_outbox WHERE event_id='rollback-event' AND state='pending'"), 2)
        let reopened = try BoazLocalDatabase(url: url)
        let settings = try await reopened.configurationEvidence()
        let counts = try await reopened.counts()
        XCTAssertEqual(settings.userVersion, 2)
        XCTAssertEqual(counts.records, 1)
        XCTAssertEqual(counts.pending, 1)
        try await reopened.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }
    #endif

    func testReadOnlyPreflightDoesNotRewriteIncompatibleDatabase() throws {
        let url = fixtureURL()
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try externalSQL(at: url, "CREATE TABLE unrelated(value INTEGER); PRAGMA user_version=42;")
        let before = SHA256.hash(data: try Data(contentsOf: url))
        XCTAssertThrowsError(try BoazLocalDatabase(url: url)) { error in
            guard let local = error as? LocalDatabaseError,
                  case .incompatibleSchema = local else {
                return XCTFail("Unexpected preflight error: \(error)")
            }
        }
        let after = SHA256.hash(data: try Data(contentsOf: url))
        XCTAssertEqual(Array(before), Array(after))
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testReadOnlyPreflightDoesNotRewriteCorruptBytes() throws {
        let url = fixtureURL()
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("not an SQLite database".utf8).write(to: url)
        let before = SHA256.hash(data: try Data(contentsOf: url))
        XCTAssertThrowsError(try BoazLocalDatabase(url: url)) { error in
            guard let local = error as? LocalDatabaseError,
                  case .incompatibleSchema = local else {
                return XCTFail("Unexpected preflight error: \(error)")
            }
        }
        let after = SHA256.hash(data: try Data(contentsOf: url))
        XCTAssertEqual(Array(before), Array(after))
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testExternalWriterInvalidatesPagedHistory() async throws {
        let url = fixtureURL()
        let ledger = try BoazLocalDatabase(url: url)
        _ = try await ledger.apply(events: [event("history-one")])
        let first = try await ledger.historyPage(typeIdentifier: SleepDerivation.rawType, limit: 1)
        try externalSQL(at: url, "UPDATE health_events SET value=value+1 WHERE event_id='history-one'")
        do {
            _ = try await ledger.historyPage(typeIdentifier: SleepDerivation.rawType,
                                             after: first.nextCursor, expectedVersion: first.version)
            XCTFail("An external writer must invalidate the history scan.")
        } catch HealthHistoryError.changedDuringRead {}
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testConcurrentPoolsReserveOneImmutableBatch() async throws {
        let url = fixtureURL()
        let first = try BoazLocalDatabase(url: url)
        let second = try BoazLocalDatabase(url: url)
        _ = try await first.apply(events: (0..<20).map { event("concurrent-\($0)") })
        async let a = first.prepareBatch(deviceID: "synthetic-device")
        async let b = second.prepareBatch(deviceID: "synthetic-device")
        let (one, two) = try await (a, b)
        XCTAssertEqual(one?.id, two?.id)
        XCTAssertEqual(one?.body, two?.body)
        XCTAssertEqual(one?.eventCount, 20)
        XCTAssertEqual(try externalInt(at: url, "SELECT COUNT(*) FROM upload_batches"), 1)
        XCTAssertEqual(try externalInt(at: url, "SELECT COUNT(*) FROM upload_outbox WHERE state='sending'"), 20)
        try await first.closeForTesting()
        try await second.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testCancelledBeforeWriteLeavesAnchorAndRowsUnchanged() async throws {
        let url = fixtureURL()
        let ledger = try BoazLocalDatabase(url: url)
        let cancelledEvent = event("cancelled")
        let typeIdentifier = SleepDerivation.rawType
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try await ledger.apply(events: [cancelledEvent], anchor: Data([9]),
                                          typeIdentifier: typeIdentifier)
        }
        do {
            _ = try await task.value
            XCTFail("Pre-cancelled collection should not commit.")
        } catch is CancellationError {}
        let counts = try await ledger.counts()
        let savedAnchor = try await ledger.anchor(for: SleepDerivation.rawType)
        XCTAssertEqual(counts.records, 0)
        XCTAssertNil(savedAnchor)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testProtectionFailureAfterCommitRetainsPageButBlocksUpload() async throws {
        let url = fixtureURL()
        let gate = ProtectionGate()
        let ledger = try BoazLocalDatabase(url: url, fileProtection: gate.verify)
        let anchor = Data([7, 8])
        do {
            _ = try await ledger.apply(events: [event("committed-before-check")],
                                       anchor: anchor, typeIdentifier: SleepDerivation.rawType)
            XCTFail("A failed post-commit protection check must be reported.")
        } catch LocalDatabaseError.committedButProtectionUnverified {}
        let counts = try await ledger.counts()
        let savedAnchor = try await ledger.anchor(for: SleepDerivation.rawType)
        let verifiedBeforeRetry = await ledger.isStorageProtectionVerified()
        XCTAssertEqual(counts.records, 1)
        XCTAssertEqual(savedAnchor, anchor)
        XCTAssertFalse(verifiedBeforeRetry)
        try await ledger.verifyStorageProtectionForUpload()
        let verifiedAfterRetry = await ledger.isStorageProtectionVerified()
        XCTAssertTrue(verifiedAfterRetry)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testHeartRateAggregationIsLimitedToDisplayedWorkouts() async throws {
        let url = fixtureURL()
        let ledger = try BoazLocalDatabase(url: url)
        let shown = UUID().uuidString
        let hidden = UUID().uuidString
        let readings = [70.0, 90.0, 110.0]
        var events = readings.enumerated().map { index, value in
            event("workout:\(shown):heart-rate:\(index)", type: "boaz.workout.heart_rate", value: value)
        }
        events.append(event("workout:\(hidden):heart-rate:0", type: "boaz.workout.heart_rate", value: 200))
        _ = try await ledger.apply(events: events)
        let summaries = try await ledger.workoutHeartRateSummaries(workoutIDs: [shown])
        XCTAssertEqual(summaries.count, 1)
        XCTAssertEqual(summaries[shown]?.average, 90)
        XCTAssertEqual(summaries[shown]?.minimum, 70)
        XCTAssertEqual(summaries[shown]?.maximum, 110)
        XCTAssertEqual(summaries[shown]?.sampleCount, 3)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testEarlierV2DatabaseGainsBoundedHeartRateIndexOnOpen() async throws {
        let url = fixtureURL()
        let id = UUID().uuidString
        let first = try BoazLocalDatabase(url: url)
        _ = try await first.apply(events: [event("workout:\(id):heart-rate:one",
                                                 type: "boaz.workout.heart_rate", value: 81)])
        try await first.closeForTesting()
        try externalSQL(at: url, "DROP INDEX health_events_workout_hr_id")
        let reopened = try BoazLocalDatabase(url: url)
        let summary = try await reopened.workoutHeartRateSummaries(workoutIDs: [id])
        XCTAssertEqual(summary[id]?.sampleCount, 1)
        XCTAssertEqual(summary[id]?.average, 81)
        XCTAssertEqual(try externalInt(at: url, """
            SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='health_events_workout_hr_id'
            """), 1)
        try await reopened.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testBoundedHeartRatePlanAndSynthetic10000And100000Timing() async throws {
        for count in [10_000, 100_000] {
            let url = fixtureURL()
            let ledger = try BoazLocalDatabase(url: url)
            let id = UUID().uuidString
            let prefix = "workout:\(id):heart-rate:"
            let seedStart = Date()
            try externalSQL(at: url, """
                WITH digits(d) AS (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9))
                INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,
                                          start_utc,end_utc,value,unit,payload,content_hash,updated_at)
                SELECT '\(prefix)' || printf('%05d',a.d*10000+b.d*1000+c.d*100+d.d*10+e.d),
                       1,'upsert','quantity','boaz.workout.heart_rate',
                       '2023-11-14T22:13:20Z','2023-11-14T22:13:21Z',60+e.d,'count/min',
                       X'7b7d','synthetic','2023-11-14T22:13:20Z'
                FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e
                LIMIT \(count);
                """)
            let seedSeconds = Date().timeIntervalSince(seedStart)
            let plan = try externalPlan(at: url, """
                SELECT AVG(value) FROM health_events INDEXED BY health_events_workout_hr_id
                WHERE operation='upsert' AND type_identifier='boaz.workout.heart_rate'
                  AND event_id>='\(prefix)' AND event_id<'workout:\(id):heart-rate;'
                  AND value IS NOT NULL AND value >= -1.7976931348623157e308
                  AND value <= 1.7976931348623157e308
                """)
            XCTAssertTrue(plan.contains { $0.contains("SEARCH") && $0.contains("health_events_workout_hr_id") },
                          "Expected indexed range search, observed \(plan)")
            let displayed = [id] + (0..<19).map { _ in UUID().uuidString }
            let queryStart = Date()
            let summaries = try await ledger.workoutHeartRateSummaries(workoutIDs: displayed)
            let querySeconds = Date().timeIntervalSince(queryStart)
            XCTAssertEqual(summaries.count, 1)
            XCTAssertEqual(summaries[id]?.sampleCount, count)
            XCTAssertEqual(summaries[id]?.minimum, 60)
            XCTAssertEqual(summaries[id]?.maximum, 69)
            XCTAssertEqual(summaries[id]?.average, 64.5)
            print("GRDB workout-heart-rate \(count): seed \(String(format: "%.3f", seedSeconds)) s, "
                + "20-ID aggregate \(String(format: "%.3f", querySeconds)) s; plan \(plan)")
            try await ledger.closeForTesting()
            try FileManager.default.removeItem(at: url.deletingLastPathComponent())
        }
    }
}
