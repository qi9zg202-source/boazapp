import Foundation
import Darwin
import SQLite3
@testable import boazapp

/// A low-overhead scheduling probe, not a visual frame-rate or touch-latency test.
@MainActor
private final class MainActorHeartbeat {
    private var previous = ContinuousClock().now
    private var maxGap = Duration.zero
    private var ticks = 0
    private var worker: Task<Void, Never>?

    func start() {
        previous = ContinuousClock().now
        worker = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: .milliseconds(100)) }
                catch { break }
                guard let self else { break }
                let now = ContinuousClock().now
                let gap = previous.duration(to: now)
                if gap > maxGap { maxGap = gap }
                previous = now
                ticks += 1
            }
        }
    }

    func stop() async -> (ticks: Int, maxGapMilliseconds: Double) {
        worker?.cancel()
        await worker?.value
        worker = nil
        let components = maxGap.components
        let milliseconds = Double(components.seconds) * 1_000 + Double(components.attoseconds) / 1_000_000_000_000_000
        return (ticks, milliseconds)
    }
}

/// Executed by XCTest with synthetic records and isolated SQLite files.
/// This does not prove HealthKit availability, effective iOS file protection, Keychain, or background delivery.
struct LocalCoreHarness {
    enum Failure: Error { case assertion(String) }

    static func memorySnapshot() -> (resident: UInt64, peakResident: Int64) {
        var information = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size / MemoryLayout<integer_t>.size)
        let status = withUnsafeMutablePointer(to: &information) { pointer in
            pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
                task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
            }
        }
        var usage = rusage()
        let peak = getrusage(RUSAGE_SELF, &usage) == 0 ? Int64(usage.ru_maxrss) : -1
        return (status == KERN_SUCCESS ? UInt64(information.resident_size) : 0, peak)
    }

    final class ProtectionProbe: @unchecked Sendable {
        private let lock = NSLock()
        private var calls = 0
        private var shouldFail = true

        func check(_ url: URL) throws {
            lock.lock()
            defer { lock.unlock() }
            calls += 1
            if calls > 1 && shouldFail { throw Failure.assertion("Synthetic file protection failure") }
        }

        func allow() {
            lock.lock()
            shouldFail = false
            lock.unlock()
        }
    }

    static func sqlite(_ path: URL, _ sql: String) throws {
        var connection: OpaquePointer?
        guard sqlite3_open_v2(path.path, &connection, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK,
              let connection else { throw Failure.assertion("Synthetic SQLite fixture could not open") }
        defer { sqlite3_close(connection) }
        var error: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(connection, sql, nil, nil, &error) == SQLITE_OK else {
            let detail = error.map { String(cString: $0) } ?? "unknown"
            if let error { sqlite3_free(error) }
            throw Failure.assertion("Synthetic SQLite fixture failed: \(detail)")
        }
    }

    static func sqliteVersion(_ path: URL) throws -> Int {
        var connection: OpaquePointer?
        guard sqlite3_open_v2(path.path, &connection, SQLITE_OPEN_READONLY, nil) == SQLITE_OK,
              let connection else { throw Failure.assertion("Synthetic SQLite version could not open") }
        defer { sqlite3_close(connection) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(connection, "PRAGMA user_version", -1, &statement, nil) == SQLITE_OK,
              let statement else { throw Failure.assertion("Synthetic SQLite version could not be read") }
        defer { sqlite3_finalize(statement) }
        guard sqlite3_step(statement) == SQLITE_ROW else { throw Failure.assertion("Synthetic SQLite version was absent") }
        return Int(sqlite3_column_int(statement, 0))
    }

    static func require(_ condition: Bool, _ message: String) throws {
        if !condition { throw Failure.assertion(message) }
    }

    static func event(_ id: String, operation: String = "upsert", value: Double? = 72,
                      kind: String = "quantity", type: String = "HKQuantityTypeIdentifierHeartRate",
                      unit: String = "count/min", metadata: [String: String] = [:]) -> HealthEvent {
        HealthEvent(eventID: id, revision: 0, operation: operation, kind: kind, type: type,
                    sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                    startUTC: operation == "upsert" ? Date(timeIntervalSince1970: 1_700_000_000) : nil,
                    endUTC: operation == "upsert" ? Date(timeIntervalSince1970: 1_700_000_001) : nil,
                    value: operation == "upsert" ? value : nil, unit: operation == "upsert" ? unit : nil,
                    metadata: metadata)
    }

    static func receipt(_ batch: PreparedHealthBatch, sequence: Int = 1) -> TokyoReceipt {
        TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: batch.eventCount,
                     receivedAt: "2026-09-18T00:00:00.123Z", projectedAt: nil,
                     contentHash: batch.contentHash, commitSequence: sequence)
    }

    static func run(schema: URL, directory: URL, recordCount: Int) async throws {
        guard (200...100_000).contains(recordCount) else { throw Failure.assertion("recordCount must be 200...100000") }
        func database(_ name: String) throws -> BoazLocalDatabase {
            try BoazLocalDatabase(url: directory.appendingPathComponent("\(name).sqlite"), schemaURL: schema)
        }
        let db = try database("atomic")
        do {
            _ = try await db.apply(events: [event("good"), event("bad", value: .nan)],
                                   anchor: Data([1]), typeIdentifier: "heart")
            throw Failure.assertion("Non-finite page must fail")
        } catch is EncodingError {}
        try require(try await db.anchor(for: "heart") == nil, "Failed page advanced anchor")
        try require(try await db.counts().records == 0, "Failed page partially committed")
        _ = try await db.apply(events: [event("one")], anchor: Data([2]), typeIdentifier: "heart")
        try require(try await db.apply(events: [event("one")], anchor: Data([3]), typeIdentifier: "heart") == 0,
                    "Duplicate sample changed revision")
        try require(try await db.anchor(for: "heart") == Data([3]), "Duplicate page lost new anchor")
        print("PASS CORE-01: page rollback, atomic anchor, duplicate page")

        guard let batch = try await db.prepareBatch(deviceID: "synthetic-device") else { throw Failure.assertion("Missing batch") }
        let reopened = try database("atomic")
        let retry = try await reopened.prepareBatch(deviceID: "synthetic-device")
        try require(retry?.id == batch.id && retry?.body == batch.body, "Reopen changed retry bytes")
        _ = try await db.apply(events: [event("one", operation: "delete")])
        try await db.markCloudSaved(batchID: batch.id, receipt: JSONEncoder().encode(receipt(batch)))
        let deletion = try await db.prepareBatch(deviceID: "synthetic-device")
        guard let deletion, let body = try JSONSerialization.jsonObject(with: deletion.body) as? [String: Any],
              let events = body["events"] as? [[String: Any]] else { throw Failure.assertion("Missing deletion") }
        try require(events.first?["operation"] as? String == "delete" && events.first?["revision"] as? Int == 2,
                    "Old receipt consumed newer deletion")
        print("PASS CORE-02: durable retry and deletion while a batch is outstanding")

        let bad = TokyoReceipt(batchID: deletion.id, status: "metrics_current", acceptedEvents: deletion.eventCount,
                               receivedAt: "2026-09-18T00:00:00Z", projectedAt: "2026-09-18T00:01:00Z",
                               contentHash: nil, commitSequence: 2)
        do {
            try await db.markCloudSaved(batchID: deletion.id, receipt: JSONEncoder().encode(bad))
            throw Failure.assertion("Missing receipt hash accepted")
        } catch TokyoGatewayError.hashMismatch {}
        do {
            try await db.markMetricsCurrent(batchID: deletion.id, receipt: bad)
            throw Failure.assertion("Unbound metrics receipt accepted")
        } catch TokyoGatewayError.hashMismatch {}
        try require(try await db.counts().pending == 1, "Invalid receipt changed pending state")
        print("PASS CORE-03: receipt evidence fails closed for saved and projected states")

        let activity = try database("activity")
        let activityID = "activity:2026-09-18:stand"
        _ = try await activity.apply(events: [event(activityID, value: 8, kind: "activity", type: "activity.stand", unit: "hours")])
        _ = try await activity.apply(events: [event(activityID, value: 9, kind: "activity", type: "activity.stand", unit: "hours")])
        let currentDay = try await activity.latestEvent(typeIdentifier: "activity.stand")
        try require(currentDay?.value == 9 && currentDay?.revision == 2, "Activity revision was not replaced")
        let wrist = event("wrist", value: 36.4, type: "HKQuantityTypeIdentifierAppleSleepingWristTemperature", unit: "degC")
        _ = try await activity.apply(events: [wrist])
        let currentWrist = try await activity.latestEvent(typeIdentifier: wrist.type)
        try require(currentWrist?.value == 36.4 && currentWrist?.unit == "degC", "Absolute wrist value changed")
        print("PASS CORE-04: daily revision and absolute wrist unit retained in ledger")

        let workoutDB = try database("workout")
        let workout = UUID()
        _ = try await workoutDB.apply(events: [event(workout.uuidString, kind: "workout", type: "HKWorkoutTypeIdentifier")])
        _ = try await workoutDB.apply(events: [event("workout:\(workout.uuidString):heart:1")])
        try await workoutDB.advanceWorkoutEvents(id: workout, nextOffset: 100, done: false)
        let progress = try await workoutDB.workoutProgress(id: workout)
        try require(progress?.offset == 100 && progress?.heartRateDone == false, "Workout progress lost")
        _ = try await workoutDB.apply(events: [event(workout.uuidString, operation: "delete", kind: "workout", type: "HKWorkoutTypeIdentifier")])
        try require(try await workoutDB.counts().records == 0, "Deleted workout retained live child")
        try require(try await workoutDB.pendingWorkoutIDs().isEmpty, "Deleted workout retained job")
        print("PASS CORE-05: workout checkpoint and cascading child tombstone")

        try await activity.requeueAfterErasure(id: "synthetic-erasure")
        let queued = try await activity.prepareBatch(deviceID: "synthetic-device")
        try await activity.requeueAfterErasure(id: "synthetic-erasure")
        let queuedAgain = try await activity.prepareBatch(deviceID: "synthetic-device")
        try require(queued?.id == queuedAgain?.id && queued?.body == queuedAgain?.body, "Erasure retry rebuilt existing queue")
        print("PASS CORE-06: local erasure recovery is idempotent")

        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let origin = Date(timeIntervalSince1970: 1_700_000_000)
        func segment(_ start: Double, _ end: Double, _ stage: SleepStage, _ source: String = "watch") -> SleepSegment {
            SleepSegment(start: origin.addingTimeInterval(start * 60), end: origin.addingTimeInterval(end * 60), stage: stage, sourceID: source)
        }
        let overlapping = [segment(0, 120, .inBed), segment(0, 60, .asleepCore), segment(30, 90, .asleepDeep), segment(90, 120, .awake)]
        guard let session = SleepAnalyzer.sessions(from: overlapping, calendar: calendar).first else { throw Failure.assertion("No sleep session") }
        try require(session.asleepSeconds == 5400 && session.stages.deepSeconds == 3600 && session.efficiency == 0.75,
                    "Sleep overlap or efficiency incorrect")
        let sessions = SleepAnalyzer.sessions(from: [segment(0, 420, .asleepCore), segment(1440, 1860, .asleepCore), segment(2160, 2220, .asleepCore)], calendar: calendar)
        try require(sessions.count == 3 && sessions.filter(\.isNap).count == 1 && sessions.allSatisfy { $0.efficiency == nil },
                    "Nights or naps were merged, or efficiency was invented")
        print("PASS CORE-07: sleep overlaps, two nights, nap, and missing in-bed denominator")

        let large = try database("large")
        let heartbeat = MainActorHeartbeat()
        await heartbeat.start()
        defer { Task { _ = await heartbeat.stop() } }
        let beforeImportMemory = memorySnapshot()
        let importStart = Date()
        var pages = 0
        for start in stride(from: 0, to: recordCount, by: 500) {
            let page = (start..<min(start + 500, recordCount)).map { event("large-\($0)", value: Double(60 + $0 % 60)) }
            let anchor = Data(String(start).utf8)
            _ = try await large.apply(events: page, anchor: anchor, typeIdentifier: "heart")
            try require(try await large.anchor(for: "heart") == anchor, "Large import anchor mismatch")
            pages += 1
        }
        let importSeconds = Date().timeIntervalSince(importStart)
        let afterImportMemory = memorySnapshot()
        let uploadStart = Date()
        var total = 0, batches = 0
        while let next = try await large.prepareBatch(deviceID: "synthetic-device") {
            try require(next.eventCount <= 200 && next.body.count <= 128 * 1024, "Batch exceeded a bound")
            let repeated = try await large.prepareBatch(deviceID: "synthetic-device")
            try require(repeated?.body == next.body, "Large import retry changed bytes")
            batches += 1
            total += next.eventCount
            try await large.markCloudSaved(batchID: next.id, receipt: JSONEncoder().encode(receipt(next, sequence: batches)))
        }
        let counts = try await large.counts()
        try require(total == recordCount && counts.records == recordCount && counts.pending == 0 && counts.cloudSaved == recordCount,
                    "Large history lost records")
        let diskBytes = try ["large.sqlite", "large.sqlite-wal", "large.sqlite-shm"].reduce(Int64(0)) { total, name in
            let path = directory.appendingPathComponent(name).path
            guard FileManager.default.fileExists(atPath: path) else { return total }
            let attributes = try FileManager.default.attributesOfItem(atPath: path)
            return total + ((attributes[.size] as? NSNumber)?.int64Value ?? 0)
        }
        let afterBatchesMemory = memorySnapshot()
        let responsiveness = await heartbeat.stop()
        print("PASS CORE-08: \(recordCount) synthetic records; \(pages) import pages; \(batches) bounded upload batches")
        print(String(format: "MEASURE synthetic_import_seconds=%.3f synthetic_batch_prepare_seconds=%.3f sqlite_with_wal_bytes=%lld process_peak_resident_bytes=%lld main_actor_heartbeat_ticks=%d main_actor_max_tick_gap_ms=%.3f", importSeconds, Date().timeIntervalSince(uploadStart), diskBytes, afterBatchesMemory.peakResident, responsiveness.ticks, responsiveness.maxGapMilliseconds))
        print("MEASURE memory_before_import_resident_bytes=\(beforeImportMemory.resident) peak_bytes=\(beforeImportMemory.peakResident)")
        print("MEASURE memory_after_import_resident_bytes=\(afterImportMemory.resident) peak_bytes=\(afterImportMemory.peakResident)")
        print("MEASURE memory_after_batches_resident_bytes=\(afterBatchesMemory.resident) peak_bytes=\(afterBatchesMemory.peakResident)")

        let legacyPath = directory.appendingPathComponent("legacy.sqlite")
        let legacy = try database("legacy")
        _ = try await legacy.apply(events: [event("legacy-record")], anchor: Data([7, 8]), typeIdentifier: "heart")
        let legacyBatch = try await legacy.prepareBatch(deviceID: "synthetic-device")
        try require(try sqliteVersion(legacyPath) == 2, "Fresh database was not versioned")
        try await legacy.closeForTesting()
        try sqlite(legacyPath, """
            DROP TRIGGER health_events_clock_insert;
            DROP TRIGGER health_events_clock_update;
            DROP TRIGGER health_events_clock_delete;
            DROP TABLE local_change_clock;
            PRAGMA user_version=0;
            """)
        let upgraded = try database("legacy")
        try require(try sqliteVersion(legacyPath) == 2, "Compatible legacy database was not recognized")
        try require(try await upgraded.counts().records == 1, "Legacy record was lost")
        try require(try await upgraded.anchor(for: "heart") == Data([7, 8]), "Legacy anchor was lost")
        let recoveredBatch = try await upgraded.prepareBatch(deviceID: "synthetic-device")
        try require(recoveredBatch?.id == legacyBatch?.id && recoveredBatch?.body == legacyBatch?.body,
                    "Legacy retry identity or bytes changed")
        print("PASS CORE-09: compatible unversioned SQLite is stamped without losing records, anchor, or queued bytes")

        try await upgraded.closeForTesting()
        try sqlite(legacyPath, "PRAGMA user_version=42")
        do {
            _ = try database("legacy")
            throw Failure.assertion("Unknown schema version opened")
        } catch LocalDatabaseError.incompatibleSchema {}
        try require(try sqliteVersion(legacyPath) == 42, "Unknown schema version was overwritten")
        let partialPath = directory.appendingPathComponent("partial.sqlite")
        try sqlite(partialPath, "CREATE TABLE health_events(event_id TEXT PRIMARY KEY)")
        do {
            _ = try database("partial")
            throw Failure.assertion("Partial unknown schema opened")
        } catch LocalDatabaseError.incompatibleSchema {}
        try require(try sqliteVersion(partialPath) == 0, "Partial database was stamped")
        let corruptPath = directory.appendingPathComponent("corrupt.sqlite")
        try Data("not a SQLite database".utf8).write(to: corruptPath)
        do {
            _ = try database("corrupt")
            throw Failure.assertion("Corrupt SQLite opened")
        } catch LocalDatabaseError.incompatibleSchema {}
        print("PASS CORE-10: future, partial, and corrupt SQLite fail visibly without a reset")

        let probe = ProtectionProbe()
        let protected = try BoazLocalDatabase(url: directory.appendingPathComponent("protection.sqlite"),
                                              schemaURL: schema, fileProtection: probe.check)
        do {
            _ = try await protected.apply(events: [event("committed")], anchor: Data([9]), typeIdentifier: "heart")
            throw Failure.assertion("Post-commit protection failure was not reported")
        } catch LocalDatabaseError.committedButProtectionUnverified {}
        try require(try await protected.counts().records == 1, "Committed record was incorrectly treated as rolled back")
        try require(try await protected.anchor(for: "heart") == Data([9]), "Committed anchor was incorrectly rolled back")
        try require(!(await protected.isStorageProtectionVerified()), "Upload protection gate stayed open")
        do {
            try await protected.verifyStorageProtectionForUpload()
            throw Failure.assertion("Unprotected upload passed")
        } catch LocalDatabaseError.storageProtectionUnverified {}
        probe.allow()
        try await protected.verifyStorageProtectionForUpload()
        try require(await protected.isStorageProtectionVerified(), "Protection retry did not reopen upload gate")
        print("PASS CORE-11: committed page remains durable while protection failure blocks upload until verified")

        try await db.closeForTesting()
        try await reopened.closeForTesting()
        try await activity.closeForTesting()
        try await workoutDB.closeForTesting()
        try await large.closeForTesting()
        try await protected.closeForTesting()
        print("RESULT 11/11 scenarios passed; all records synthetic; no remote calls")
    }
}
