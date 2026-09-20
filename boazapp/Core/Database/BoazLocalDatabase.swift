import CryptoKit
import Foundation
import GRDB

enum LocalDatabaseError: Error, LocalizedError {
    case open(String)
    case query(String)
    case missingSchema
    case incompatibleSchema(String)
    case storageProtectionUnverified(String)
    case committedButProtectionUnverified(String)
    case oversizedEvent

    var errorDescription: String? {
        switch self {
        case .open(let detail), .query(let detail): detail
        case .missingSchema: "The local database schema is missing from the app."
        case .incompatibleSchema(let detail): "The local health database was left unchanged because its schema or contents are incompatible: \(detail)"
        case .storageProtectionUnverified(let detail): "Local file protection could not be verified. Upload is blocked: \(detail)"
        case .committedButProtectionUnverified(let detail): "Health records were committed locally, but file protection could not be verified. Upload is blocked: \(detail)"
        case .oversizedEvent: "A health event exceeds the safe upload limit."
        }
    }
}

struct PreparedHealthBatch: Sendable {
    let id: String
    let body: Data
    let contentHash: String
    let eventCount: Int
}

struct LocalHealthCounts: Sendable {
    let records: Int
    let pending: Int
    let cloudSaved: Int
    let metricsCurrent: Int
}

struct LocalAuditEntry: Sendable, Identifiable {
    let id: Int64
    let phase: String
    let outcome: String
    let detail: String
    let eventCount: Int
    let createdAt: Date
}

struct WorkoutHeartRateSummary: Sendable {
    let average: Double
    let minimum: Double
    let maximum: Double
    let sampleCount: Int
}

struct LocalDatabaseConfigurationEvidence: Sendable {
    let journalMode: String
    let synchronous: Int
    let foreignKeys: Int
    let userVersion: Int
}

/// All writes use GRDB's single serialized writer. Reads use its bounded WAL
/// snapshot pool; neither a SQLite handle nor a mutable Row escapes a closure.
actor BoazLocalDatabase {
    private static let schemaVersion = 2
    private let pool: DatabasePool
    private let url: URL
    private let fileProtection: @Sendable (URL) throws -> Void
    private var storageProtectionVerified = true

    init(url: URL? = nil, schemaURL: URL? = nil,
         fileProtection: (@Sendable (URL) throws -> Void)? = nil) throws {
        try self.init(url: url, schemaURL: schemaURL, fileProtection: fileProtection,
                      internalMigrationProbe: nil)
    }

    #if DEBUG
    /// Only XCTest/debug builds can inject a failure after migration DDL but
    /// before the user_version stamp, proving GRDB rolls the whole step back.
    init(url: URL, schemaURL: URL? = nil,
         fileProtection: (@Sendable (URL) throws -> Void)? = nil,
         migrationFailureProbe: @escaping @Sendable () throws -> Void) throws {
        try self.init(url: url, schemaURL: schemaURL, fileProtection: fileProtection,
                      internalMigrationProbe: migrationFailureProbe)
    }
    #endif

    private init(url: URL?, schemaURL: URL?,
                 fileProtection: (@Sendable (URL) throws -> Void)?,
                 internalMigrationProbe: (@Sendable () throws -> Void)?) throws {
        let target: URL
        if let url {
            target = url
        } else {
            let directory = try FileManager.default.url(
                for: .applicationSupportDirectory, in: .userDomainMask,
                appropriateFor: nil, create: true
            ).appendingPathComponent("BoazHealth", isDirectory: true)
            target = directory.appendingPathComponent("health.sqlite")
        }
        try FileManager.default.createDirectory(at: target.deletingLastPathComponent(), withIntermediateDirectories: true)
        #if os(iOS)
        try FileManager.default.setAttributes(
            [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
            ofItemAtPath: target.deletingLastPathComponent().path)
        var protectedDirectory = target.deletingLastPathComponent()
        var backupPolicy = URLResourceValues()
        backupPolicy.isExcludedFromBackup = true
        try protectedDirectory.setResourceValues(backupPolicy)
        #endif

        var configuration = Configuration()
        configuration.journalMode = .wal
        configuration.foreignKeysEnabled = true
        configuration.maximumReaderCount = 4
        configuration.busyMode = .timeout(5)
        let existingFile = FileManager.default.fileExists(atPath: target.path)
        if existingFile {
            // DatabasePool configures WAL while opening. Inspect an existing
            // file read-only first, so incompatible input is rejected before
            // the pooled writer can change its journal header or schema.
            try Self.preflightExistingSchema(at: target)
        }
        let pool: DatabasePool
        do {
            pool = try DatabasePool(path: target.path, configuration: configuration)
        } catch {
            if existingFile {
                throw LocalDatabaseError.incompatibleSchema("Could not open the existing database: \(error.localizedDescription)")
            }
            throw LocalDatabaseError.open(error.localizedDescription)
        }
        self.pool = pool
        self.url = target
        self.fileProtection = fileProtection ?? Self.protectFiles

        // GRDB's WAL setup selects NORMAL. FULL is an explicit durability
        // choice for this health ledger and must be set on its writer before
        // any app transaction. A failed read-back fails closed.
        try pool.writeWithoutTransaction { db in
            try db.execute(sql: "PRAGMA synchronous=FULL")
            guard try Int.fetchOne(db, sql: "PRAGMA synchronous") == 2,
                  try String.fetchOne(db, sql: "PRAGMA journal_mode")?.lowercased() == "wal",
                  try Int.fetchOne(db, sql: "PRAGMA foreign_keys") == 1 else {
                throw LocalDatabaseError.open("WAL, FULL durability, or foreign-key enforcement was not effective.")
            }
        }
        try pool.write { db in
            try Self.openCompatibleSchema(db, schemaURL: schemaURL,
                                          migrationFailureProbe: internalMigrationProbe)
        }
        try pool.writeWithoutTransaction { db in
            guard try Int.fetchOne(db, sql: "PRAGMA synchronous") == 2,
                  try Int.fetchOne(db, sql: "PRAGMA user_version") == Self.schemaVersion else {
                throw LocalDatabaseError.open("Durability or schema version changed during database setup.")
            }
        }
        try self.fileProtection(target)
    }

    /// A committed page remains local if protection verification fails. Upload
    /// is blocked until the database and current WAL sidecars pass again.
    func verifyStorageProtectionForUpload() throws {
        do {
            try fileProtection(url)
            storageProtectionVerified = true
        } catch {
            storageProtectionVerified = false
            throw LocalDatabaseError.storageProtectionUnverified(error.localizedDescription)
        }
    }

    func isStorageProtectionVerified() -> Bool { storageProtectionVerified }

    func anchor(for typeIdentifier: String) throws -> Data? {
        try pool.read { db in
            try Self.row(db, "SELECT anchor FROM query_anchors WHERE type_identifier=?", [.text(typeIdentifier)])?["anchor"]
        }
    }

    @discardableResult
    func apply(events: [HealthEvent], anchor: Data? = nil, typeIdentifier: String? = nil,
               expectedHistoryVersion: HealthHistoryVersion? = nil) async throws -> Int {
        try Task.checkCancellation()
        let result = try await pool.write { db in
            try Self.apply(db, events: events, anchor: anchor, typeIdentifier: typeIdentifier,
                           expectedHistoryVersion: expectedHistoryVersion)
        }
        try verifyProtectionAfterCommit()
        return result
    }

    private static func apply(_ db: Database, events: [HealthEvent], anchor: Data?,
                              typeIdentifier: String?, expectedHistoryVersion: HealthHistoryVersion?) throws -> Int {
        if let expectedHistoryVersion { try validateHistoryVersion(db, expectedHistoryVersion) }
        var changed = 0
        for event in events {
            let incoming = try encoder().encode(event)
            let contentHash = hash(incoming)
            let previous = try row(db,
                "SELECT revision,content_hash FROM health_events WHERE event_id=?", [.text(event.eventID)])
            let priorHash: String? = previous?["content_hash"]
            if priorHash == contentHash { continue }
            let priorRevision: Int = previous?["revision"] ?? 0
            let revision = max(event.revision, priorRevision + 1, 1)
            var object = try JSONSerialization.jsonObject(with: incoming) as? [String: Any] ?? [:]
            object["revision"] = revision
            let stored = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
            let now = timestamp(Date())
            try execute(db, """
                INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,start_utc,end_utc,value,unit,payload,content_hash,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision,operation=excluded.operation,kind=excluded.kind,
                type_identifier=excluded.type_identifier,start_utc=excluded.start_utc,end_utc=excluded.end_utc,value=excluded.value,
                unit=excluded.unit,payload=excluded.payload,content_hash=excluded.content_hash,updated_at=excluded.updated_at
                """, [
                    .text(event.eventID), .int(revision), .text(event.operation), .text(event.kind), .text(event.type),
                    event.startUTC.map { .text(timestamp($0)) } ?? .null,
                    event.endUTC.map { .text(timestamp($0)) } ?? .null,
                    event.value.map { .double($0) } ?? .null,
                    event.unit.map { .text($0) } ?? .null,
                    .blob(stored), .text(contentHash), .text(now)
                ])
            try execute(db, "DELETE FROM upload_outbox WHERE event_id=?", [.text(event.eventID)])
            try execute(db, "INSERT INTO upload_outbox(event_id,revision,state,updated_at) VALUES(?,?,'pending',?)",
                        [.text(event.eventID), .int(revision), .text(now)])
            if event.kind == "workout", UUID(uuidString: event.eventID) != nil {
                changed += try tombstoneWorkoutDetails(db, id: event.eventID)
                try execute(db, "DELETE FROM local_control_state WHERE key=?", [.text("workout-retry:\(event.eventID)")])
                try execute(db, "DELETE FROM query_anchors WHERE type_identifier=?", [.text("workout-heart-rate:\(event.eventID)")])
                if event.operation == "upsert" {
                    try execute(db, """
                        INSERT INTO workout_detail_jobs(workout_id,event_offset,events_done,heart_rate_done,updated_at)
                        VALUES(?,0,0,0,?) ON CONFLICT(workout_id) DO UPDATE SET
                        event_offset=0,events_done=0,heart_rate_done=0,updated_at=excluded.updated_at
                        """, [.text(event.eventID), .text(now)])
                } else {
                    try execute(db, "DELETE FROM workout_detail_jobs WHERE workout_id=?", [.text(event.eventID)])
                }
            }
            changed += 1
        }
        if let anchor, let typeIdentifier {
            try execute(db, """
                INSERT INTO query_anchors(type_identifier,anchor,updated_at) VALUES(?,?,?)
                ON CONFLICT(type_identifier) DO UPDATE SET anchor=excluded.anchor,updated_at=excluded.updated_at
                """, [.text(typeIdentifier), .blob(anchor), .text(timestamp(Date()))])
        }
        try writeAudit(db, phase: "collect", outcome: "local_saved", detail: typeIdentifier ?? "activity", count: changed)
        return changed
    }

    /// Selection and reservation share one IMMEDIATE writer transaction. A
    /// concurrent process cannot reserve a row changed after selection.
    func prepareBatch(deviceID: String) throws -> PreparedHealthBatch? {
        // JSONSerialization creates autoreleased Foundation objects for every
        // size probe. Drain them once per batch rather than retaining all
        // probes across a long-running sync session's outer autorelease pool.
        try autoreleasepool { try transaction { db in
            if let prior = try Self.row(db, """
                SELECT batch_id,body,body_hash,next_attempt_at FROM upload_batches
                WHERE state='pending' ORDER BY created_at LIMIT 1
                """) {
                let retryAt: String? = prior["next_attempt_at"]
                if let retryAt, retryAt > Self.timestamp(Date()) { return nil }
                let id: String = prior["batch_id"]
                let body: Data = prior["body"]
                let hash: String = prior["body_hash"]
                guard let object = try JSONSerialization.jsonObject(with: body) as? [String: Any],
                      let events = object["events"] as? [[String: Any]], Self.hash(body) == hash else {
                    throw LocalDatabaseError.query("Persisted upload batch is malformed or its hash changed.")
                }
                return PreparedHealthBatch(id: id, body: body, contentHash: hash, eventCount: events.count)
            }
            let rows = try Self.rows(db, """
                SELECT o.event_id,o.revision,e.payload FROM upload_outbox o JOIN health_events e ON e.event_id=o.event_id
                WHERE o.state='pending' AND o.batch_id IS NULL ORDER BY e.updated_at,o.event_id LIMIT 200
                """)
            guard !rows.isEmpty else { return nil }
            let id = UUID().uuidString.lowercased()
            var selected: [(String, Int, [String: Any])] = []
            var body = Data()
            for row in rows {
                let payload: Data = row["payload"]
                let event = try Self.decoder().decode(HealthEvent.self, from: payload)
                let eventID: String = row["event_id"]
                let revision: Int = row["revision"]
                let candidate = selected + [(eventID, revision, Self.wire(event: event))]
                let object: [String: Any] = ["schema_version": 1, "device_id": deviceID,
                                              "batch_id": id, "events": candidate.map { $0.2 }]
                let data = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
                if data.count > 128 * 1024 {
                    if selected.isEmpty { throw LocalDatabaseError.oversizedEvent }
                    break
                }
                selected = candidate
                body = data
            }
            let hash = Self.hash(body)
            let now = Self.timestamp(Date())
            try Self.execute(db, """
                INSERT INTO upload_batches(batch_id,body,body_hash,state,created_at,updated_at)
                VALUES(?,?,?,'pending',?,?)
                """, [.text(id), .blob(body), .text(hash), .text(now), .text(now)])
            for (eventID, revision, _) in selected {
                try Self.execute(db, """
                    UPDATE upload_outbox SET batch_id=?,state='sending',updated_at=?
                    WHERE event_id=? AND revision=? AND state='pending' AND batch_id IS NULL
                    """, [.text(id), .text(now), .text(eventID), .int(revision)])
                guard db.changesCount == 1 else { throw LocalDatabaseError.query("Outbox changed during batch reservation.") }
            }
            return PreparedHealthBatch(id: id, body: body, contentHash: hash, eventCount: selected.count)
        } }
    }

    func markCloudSaved(batchID: String, receipt: Data) throws {
        try transaction { db in
            let decoded = try Self.decoder().decode(TokyoReceipt.self, from: receipt)
            try decoded.validate(for: Self.storedBatch(db, id: batchID))
            let now = Self.timestamp(Date())
            try Self.execute(db, "UPDATE upload_batches SET state='cloud_saved',receipt=?,updated_at=? WHERE batch_id=?",
                             [.blob(receipt), .text(now), .text(batchID)])
            try Self.execute(db, "UPDATE upload_outbox SET state='cloud_saved',updated_at=? WHERE batch_id=?",
                             [.text(now), .text(batchID)])
            try Self.writeAudit(db, phase: "upload", outcome: "cloud_saved", detail: batchID, count: decoded.acceptedEvents)
        }
    }

    func markMetricsCurrent(batchID: String, receipt: TokyoReceipt) throws {
        try transaction { db in
            try receipt.validate(for: Self.storedBatch(db, id: batchID))
            guard receipt.status == "metrics_current" else { throw TokyoGatewayError.unexpectedResponse }
            let now = Self.timestamp(Date())
            try Self.execute(db, "UPDATE upload_batches SET state='metrics_current',updated_at=? WHERE batch_id=?",
                             [.text(now), .text(batchID)])
            try Self.execute(db, "UPDATE upload_outbox SET state='metrics_current',updated_at=? WHERE batch_id=?",
                             [.text(now), .text(batchID)])
            try Self.writeAudit(db, phase: "project", outcome: "metrics_current", detail: batchID, count: 0)
        }
    }

    private static func storedBatch(_ db: Database, id: String) throws -> PreparedHealthBatch {
        guard let row = try row(db, "SELECT body,body_hash FROM upload_batches WHERE batch_id=?", [.text(id)]) else {
            throw TokyoGatewayError.unexpectedResponse
        }
        let body: Data = row["body"]
        let hash: String = row["body_hash"]
        guard let object = try JSONSerialization.jsonObject(with: body) as? [String: Any],
              let events = object["events"] as? [[String: Any]], hash == Self.hash(body) else {
            throw TokyoGatewayError.unexpectedResponse
        }
        return PreparedHealthBatch(id: id, body: body, contentHash: hash, eventCount: events.count)
    }

    /// An erasure resets cloud receipts while retaining the local snapshot.
    /// The marker makes recovery after a lost response safe to repeat.
    func requeueAfterErasure(id: String) throws {
        try transaction { db in
            let prior: String? = try Self.row(db,
                "SELECT value FROM local_control_state WHERE key='last_erasure_requeue'")?["value"]
            guard prior != id else { return }
            try Self.execute(db, "DELETE FROM upload_outbox; DELETE FROM upload_batches;")
            let now = Self.timestamp(Date())
            try Self.execute(db, """
                INSERT INTO upload_outbox(event_id,revision,state,updated_at)
                SELECT event_id,revision,'pending',? FROM health_events
                """, [.text(now)])
            try Self.execute(db, """
                INSERT INTO local_control_state(key,value) VALUES('last_erasure_requeue',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """, [.text(id)])
            try Self.writeAudit(db, phase: "erase", outcome: "local_requeued", detail: id, count: 0)
        }
    }

    func recordErasureStatus(id: String, status: String) throws {
        try transaction { db in
            let value = "\(id):\(status)"
            let prior: String? = try Self.row(db,
                "SELECT value FROM local_control_state WHERE key='last_erasure_status'")?["value"]
            guard prior != value else { return }
            try Self.execute(db, """
                INSERT INTO local_control_state(key,value) VALUES('last_erasure_status',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """, [.text(value)])
            try Self.writeAudit(db, phase: "erase", outcome: status, detail: id, count: 0)
        }
    }

    func lastErasureCompleted() throws -> Bool {
        try pool.read { db in
            let value: String? = try Self.row(db,
                "SELECT value FROM local_control_state WHERE key='last_erasure_status'")?["value"]
            return value?.hasSuffix(":complete") == true
        }
    }

    func deferBatch(batchID: String, error: String) throws {
        try transaction { db in
            let row = try Self.row(db, "SELECT attempts FROM upload_batches WHERE batch_id=?", [.text(batchID)])
            let attempts: Int = row?["attempts"] ?? 0
            let next = min(3600, Int(pow(2.0, Double(min(attempts + 1, 11))))) + Int.random(in: 0...3)
            try Self.execute(db, """
                UPDATE upload_batches SET attempts=attempts+1,next_attempt_at=?,last_error=?,updated_at=? WHERE batch_id=?
                """, [.text(Self.timestamp(Date().addingTimeInterval(Double(next)))),
                      .text(String(error.prefix(300))), .text(Self.timestamp(Date())), .text(batchID)])
            try Self.writeAudit(db, phase: "upload", outcome: "retry_queued", detail: batchID, count: 0)
        }
    }

    func cloudSavedBatchIDs() throws -> [String] {
        try pool.read { db in
            try Self.rows(db, "SELECT batch_id FROM upload_batches WHERE state='cloud_saved' ORDER BY created_at LIMIT 20")
                .map { row in let id: String = row["batch_id"]; return id }
        }
    }

    func recordFailure(phase: String, detail: String) throws {
        try transaction { db in
            try Self.writeAudit(db, phase: phase, outcome: "failed", detail: String(detail.prefix(300)), count: 0)
        }
    }

    func counts() throws -> LocalHealthCounts {
        try pool.read { db in
            func count(_ sql: String) throws -> Int { try Int.fetchOne(db, sql: sql) ?? 0 }
            return try LocalHealthCounts(
                records: count("SELECT COUNT(*) FROM health_events WHERE operation='upsert'"),
                pending: count("SELECT COUNT(*) FROM upload_outbox WHERE state IN ('pending','sending')"),
                cloudSaved: count("SELECT COUNT(*) FROM upload_outbox WHERE state='cloud_saved'"),
                metricsCurrent: count("SELECT COUNT(*) FROM upload_outbox WHERE state='metrics_current'")
            )
        }
    }

    func recentEvents(since: Date, limit: Int = 10_000) throws -> [HealthEvent] {
        try pool.read { db in
            try Self.rows(db, """
                SELECT payload FROM health_events WHERE operation='upsert' AND start_utc>=?
                ORDER BY start_utc DESC LIMIT ?
                """, [.text(Self.timestamp(since)), .int(limit)]).map(Self.decodeEvent)
        }
    }

    func latestEvent(typeIdentifier: String) throws -> HealthEvent? {
        try pool.read { db in
            guard let row = try Self.row(db, """
                SELECT payload FROM health_events WHERE operation='upsert' AND type_identifier=?
                ORDER BY start_utc DESC LIMIT 1
                """, [.text(typeIdentifier)]) else { return nil }
            return try Self.decodeEvent(row)
        }
    }

    func events(typeIdentifier: String, since: Date, limit: Int = 10_000) throws -> [HealthEvent] {
        try pool.read { db in
            try Self.rows(db, """
                SELECT payload FROM health_events WHERE operation='upsert' AND type_identifier=?
                AND start_utc>=? ORDER BY start_utc DESC LIMIT ?
                """, [.text(typeIdentifier), .text(Self.timestamp(since)), .int(limit)]).map(Self.decodeEvent)
        }
    }

    /// Aggregate the complete selected interval inside SQLite. A recent-row
    /// limit could allow later readings to displace the selected night.
    func averageValue(typeIdentifier: String, from start: Date, through end: Date) throws -> Double? {
        try pool.read { db in
            let row = try Self.row(db, """
                SELECT AVG(value) AS average FROM health_events
                WHERE operation='upsert' AND type_identifier=? AND start_utc>=? AND start_utc<=?
                """, [.text(typeIdentifier), .text(Self.timestamp(start)), .text(Self.timestamp(end))])
            let average: Double? = row?["average"]
            guard let average else { return nil }
            guard average.isFinite else { throw LocalDatabaseError.query("Non-finite interval average") }
            return average
        }
    }

    /// The dashboard passes only its displayed workout IDs (at most 20), so
    /// large per-workout sample histories never reach the main actor.
    func workoutHeartRateSummaries(workoutIDs: [String]) throws -> [String: WorkoutHeartRateSummary] {
        guard workoutIDs.count <= 20 else { throw LocalDatabaseError.query("Too many displayed workouts.") }
        return try pool.read { db in
            var summaries: [String: WorkoutHeartRateSummary] = [:]
            for id in Set(workoutIDs) {
                guard UUID(uuidString: id) != nil else { continue }
                let prefix = "workout:\(id):heart-rate:"
                let row = try Self.row(db, """
                    SELECT AVG(value) AS average, MIN(value) AS minimum,
                           MAX(value) AS maximum, COUNT(*) AS sample_count
                    FROM health_events INDEXED BY health_events_workout_hr_id
                    WHERE operation='upsert' AND type_identifier='boaz.workout.heart_rate'
                      AND event_id>=? AND event_id<? AND value IS NOT NULL
                      AND value >= -1.7976931348623157e308 AND value <= 1.7976931348623157e308
                    """, [.text(prefix), .text("workout:\(id):heart-rate;")])
                let count: Int = row?["sample_count"] ?? 0
                guard count > 0 else { continue }
                let average: Double = row!["average"]
                let minimum: Double = row!["minimum"]
                let maximum: Double = row!["maximum"]
                guard average.isFinite, minimum.isFinite, maximum.isFinite else {
                    throw LocalDatabaseError.query("Non-finite workout heart-rate aggregate.")
                }
                summaries[id] = WorkoutHeartRateSummary(
                    average: average, minimum: minimum, maximum: maximum, sampleCount: count)
            }
            return summaries
        }
    }

    func audit(limit: Int = 30) throws -> [LocalAuditEntry] {
        try pool.read { db in
            try Self.rows(db, """
                SELECT id,phase,outcome,detail,event_count,created_at
                FROM sync_audit_log ORDER BY id DESC LIMIT ?
                """, [.int(limit)]).map { row in
                let id: Int64 = row["id"]
                let phase: String = row["phase"]
                let outcome: String = row["outcome"]
                let detail: String = row["detail"]
                let count: Int = row["event_count"]
                let date: String = row["created_at"]
                return LocalAuditEntry(id: id, phase: phase, outcome: outcome, detail: detail,
                                       eventCount: count, createdAt: Self.parseDate(date) ?? .distantPast)
            }
        }
    }

    func pendingWorkoutIDs(limit: Int = 100, now: Date = Date()) throws -> [UUID] {
        try pool.read { db in
            try Self.rows(db, """
                SELECT w.workout_id FROM workout_detail_jobs w
                LEFT JOIN local_control_state r ON r.key='workout-retry:' || w.workout_id
                WHERE (w.events_done=0 OR w.heart_rate_done=0) AND (r.value IS NULL OR r.value<=?)
                ORDER BY w.updated_at,w.workout_id LIMIT ?
                """, [.text(Self.timestamp(now)), .int(limit)]).compactMap { row in
                let id: String = row["workout_id"]
                return UUID(uuidString: id)
            }
        }
    }

    func deferWorkoutDetails(id: UUID, until: Date, error: String) throws {
        try transaction { db in
            try Self.execute(db, """
                INSERT INTO local_control_state(key,value) VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """, [.text("workout-retry:\(id.uuidString)"), .text(Self.timestamp(until))])
            try Self.writeAudit(db, phase: "collect", outcome: "workout_deferred",
                                detail: String("\(id.uuidString): \(error)".prefix(300)), count: 0)
        }
    }

    func clearWorkoutRetry(id: UUID) throws {
        try transaction { db in
            try Self.execute(db, "DELETE FROM local_control_state WHERE key=?", [.text("workout-retry:\(id.uuidString)")])
        }
    }

    func workoutQueueCounts(now: Date = Date()) throws -> (pending: Int, deferred: Int) {
        try pool.read { db in
            let row = try Self.row(db, """
                SELECT COUNT(*) AS pending,COALESCE(SUM(CASE WHEN r.value>? THEN 1 ELSE 0 END),0) AS deferred
                FROM workout_detail_jobs w LEFT JOIN local_control_state r ON r.key='workout-retry:' || w.workout_id
                WHERE w.events_done=0 OR w.heart_rate_done=0
                """, [.text(Self.timestamp(now))])
            let pending: Int = row?["pending"] ?? 0
            let deferred: Int = row?["deferred"] ?? 0
            return (pending, deferred)
        }
    }

    func workoutProgress(id: UUID) throws -> (offset: Int, eventsDone: Bool, heartRateDone: Bool)? {
        try pool.read { db in
            guard let row = try Self.row(db, """
                SELECT event_offset,events_done,heart_rate_done FROM workout_detail_jobs WHERE workout_id=?
                """, [.text(id.uuidString)]) else { return nil }
            let offset: Int = row["event_offset"]
            let eventsDone: Int = row["events_done"]
            let heartRateDone: Int = row["heart_rate_done"]
            return (offset, eventsDone != 0, heartRateDone != 0)
        }
    }

    func advanceWorkoutEvents(id: UUID, nextOffset: Int, done: Bool) throws {
        try transaction { db in
            try Self.execute(db, """
                UPDATE workout_detail_jobs SET event_offset=?,events_done=?,updated_at=? WHERE workout_id=?
                """, [.int(nextOffset), .int(done ? 1 : 0), .text(Self.timestamp(Date())), .text(id.uuidString)])
        }
    }

    func markWorkoutHeartRateDone(id: UUID) throws {
        try transaction { db in
            try Self.execute(db, "UPDATE workout_detail_jobs SET heart_rate_done=1,updated_at=? WHERE workout_id=?",
                             [.text(Self.timestamp(Date())), .text(id.uuidString)])
        }
    }

    /// Synthetic tests must release all pool connections before unlinking a
    /// temporary fixture directory; this must never be used for live erasure.
    func closeForTesting() throws { try pool.close() }

    /// Read from the writer connection: synchronous is connection-local, so
    /// checking a separate SQLite connection would not prove write durability.
    func configurationEvidence() throws -> LocalDatabaseConfigurationEvidence {
        try pool.writeWithoutTransaction { db in
            guard let journalMode = try String.fetchOne(db, sql: "PRAGMA journal_mode"),
                  let synchronous = try Int.fetchOne(db, sql: "PRAGMA synchronous"),
                  let foreignKeys = try Int.fetchOne(db, sql: "PRAGMA foreign_keys"),
                  let userVersion = try Int.fetchOne(db, sql: "PRAGMA user_version") else {
                throw LocalDatabaseError.query("Could not read writer configuration.")
            }
            return LocalDatabaseConfigurationEvidence(journalMode: journalMode, synchronous: synchronous,
                                                      foreignKeys: foreignKeys, userVersion: userVersion)
        }
    }

    private static func writeAudit(_ db: Database, phase: String, outcome: String,
                                   detail: String, count: Int) throws {
        try execute(db, """
            INSERT INTO sync_audit_log(phase,outcome,detail,event_count,created_at) VALUES(?,?,?,?,?)
            """, [.text(phase), .text(outcome), .text(detail), .int(count), .text(timestamp(Date()))])
    }

    private static func tombstoneWorkoutDetails(_ db: Database, id: String) throws -> Int {
        let children = try rows(db, """
            SELECT event_id,revision,kind,type_identifier FROM health_events
            WHERE event_id LIKE ? AND operation='upsert'
            """, [.text("workout:\(id):%")])
        let now = timestamp(Date())
        for child in children {
            let eventID: String = child["event_id"]
            let oldRevision: Int = child["revision"]
            let kind: String = child["kind"]
            let type: String = child["type_identifier"]
            let revision = oldRevision + 1
            let deleted = HealthEvent(eventID: eventID, revision: revision, operation: "delete",
                                      kind: kind, type: type, sourceBundleID: "", sourceName: "",
                                      startUTC: nil, endUTC: nil, value: nil, unit: nil,
                                      metadata: ["workout_id": id])
            let payload = try encoder().encode(deleted)
            try execute(db, """
                UPDATE health_events SET revision=?,operation='delete',start_utc=NULL,end_utc=NULL,value=NULL,
                unit=NULL,payload=?,content_hash=?,updated_at=? WHERE event_id=?
                """, [.int(revision), .blob(payload), .text(hash(payload)), .text(now), .text(eventID)])
            try execute(db, "DELETE FROM upload_outbox WHERE event_id=?", [.text(eventID)])
            try execute(db, "INSERT INTO upload_outbox(event_id,revision,state,updated_at) VALUES(?,?,'pending',?)",
                        [.text(eventID), .int(revision), .text(now)])
        }
        return children.count
    }

    private enum BindValue {
        case text(String), int(Int), double(Double), blob(Data), null

        var databaseValue: DatabaseValue {
            switch self {
            case .text(let value): value.databaseValue
            case .int(let value): value.databaseValue
            case .double(let value): value.databaseValue
            case .blob(let value): value.databaseValue
            case .null: .null
            }
        }
    }

    private static func execute(_ db: Database, _ sql: String, _ values: [BindValue] = []) throws {
        try db.execute(sql: sql, arguments: StatementArguments(values.map(\.databaseValue)))
    }

    private static func row(_ db: Database, _ sql: String, _ values: [BindValue] = []) throws -> Row? {
        try Row.fetchOne(db, sql: sql, arguments: StatementArguments(values.map(\.databaseValue)))
    }

    private static func rows(_ db: Database, _ sql: String, _ values: [BindValue] = []) throws -> [Row] {
        try Row.fetchAll(db, sql: sql, arguments: StatementArguments(values.map(\.databaseValue)))
    }

    /// GRDB's synchronous write performs one IMMEDIATE transaction. File
    /// protection follows the returned successful COMMIT, even if the caller
    /// requests cancellation at that boundary.
    private func transaction<T>(_ body: (Database) throws -> T) throws -> T {
        try Task.checkCancellation()
        let result = try pool.write(body)
        try verifyProtectionAfterCommit()
        return result
    }

    private func verifyProtectionAfterCommit() throws {
        do {
            try fileProtection(url)
            storageProtectionVerified = true
        } catch {
            storageProtectionVerified = false
            throw LocalDatabaseError.committedButProtectionUnverified(error.localizedDescription)
        }
    }

    private static func openCompatibleSchema(
        _ db: Database, schemaURL: URL?, migrationFailureProbe: (@Sendable () throws -> Void)?
    ) throws {
        let (version, objects) = try inspectSchemaState(db)
        if objects.isEmpty {
            guard version == 0 else {
                throw LocalDatabaseError.incompatibleSchema("Versioned database has no app tables.")
            }
            guard let selectedURL = schemaURL ?? Bundle.main.url(forResource: "Schema", withExtension: "sql"),
                  let schema = try? String(contentsOf: selectedURL, encoding: .utf8) else {
                throw LocalDatabaseError.missingSchema
            }
            try db.execute(sql: schema)
            try validateSchema(db, expectedVersion: schemaVersion)
            try ensureWorkoutHeartRateIndex(db)
            try db.execute(sql: "PRAGMA user_version=\(schemaVersion)")
            return
        }
        try validateSchema(db, expectedVersion: version == schemaVersion ? schemaVersion : 1)
        if version < schemaVersion {
            try db.execute(sql: changeClockSchema)
            #if DEBUG
            try migrationFailureProbe?()
            #endif
            try validateSchema(db, expectedVersion: schemaVersion)
            try db.execute(sql: "PRAGMA user_version=\(schemaVersion)")
        }
        // A previous local v2 database can safely gain the bounded query index.
        try ensureWorkoutHeartRateIndex(db)
    }

    private static func preflightExistingSchema(at url: URL) throws {
        var configuration = Configuration()
        configuration.readonly = true
        configuration.foreignKeysEnabled = true
        do {
            let queue = try DatabaseQueue(path: url.path, configuration: configuration)
            defer { try? queue.close() }
            try queue.read { db in
                let (version, objects) = try inspectSchemaState(db)
                if objects.isEmpty {
                    guard version == 0 else {
                        throw LocalDatabaseError.incompatibleSchema("Versioned database has no app tables.")
                    }
                } else {
                    try validateSchema(db, expectedVersion: version == schemaVersion ? schemaVersion : 1)
                }
            }
        } catch let error as LocalDatabaseError {
            throw error
        } catch {
            throw LocalDatabaseError.incompatibleSchema("Could not inspect the existing database read-only: \(error.localizedDescription)")
        }
    }

    private static func inspectSchemaState(_ db: Database) throws -> (version: Int, objects: [String]) {
        let check = try String.fetchAll(db, sql: "PRAGMA quick_check(1)")
        guard check == ["ok"] else {
            throw LocalDatabaseError.incompatibleSchema("SQLite integrity check failed: \(check.first ?? "no result").")
        }
        guard try Row.fetchOne(db, sql: "PRAGMA foreign_key_check") == nil else {
            throw LocalDatabaseError.incompatibleSchema("SQLite foreign-key check failed.")
        }
        guard let version = try Int.fetchOne(db, sql: "PRAGMA user_version"),
              (0...schemaVersion).contains(version) else {
            throw LocalDatabaseError.incompatibleSchema("Unsupported schema version; expected 0, 1, or 2.")
        }
        let objects = try String.fetchAll(db, sql: """
            SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name
            """)
        return (version, objects)
    }

    private static let changeClockSchema = """
        CREATE TABLE local_change_clock (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          generation INTEGER NOT NULL CHECK (generation >= 0)
        );
        INSERT INTO local_change_clock(id,generation) VALUES(1,0);
        CREATE TRIGGER health_events_clock_insert AFTER INSERT ON health_events
        BEGIN UPDATE local_change_clock SET generation=generation+1 WHERE id=1; END;
        CREATE TRIGGER health_events_clock_update AFTER UPDATE ON health_events
        BEGIN UPDATE local_change_clock SET generation=generation+1 WHERE id=1; END;
        CREATE TRIGGER health_events_clock_delete AFTER DELETE ON health_events
        BEGIN UPDATE local_change_clock SET generation=generation+1 WHERE id=1; END;
        """

    private static func ensureWorkoutHeartRateIndex(_ db: Database) throws {
        try db.execute(sql: """
            CREATE INDEX IF NOT EXISTS health_events_workout_hr_id ON health_events(event_id,value)
            WHERE operation='upsert' AND type_identifier='boaz.workout.heart_rate'
            """)
        let columns = try rows(db, "PRAGMA index_info(\"health_events_workout_hr_id\")").map { row -> String in
            let name: String = row["name"]
            return name
        }
        let definition = try String.fetchOne(db, sql: """
            SELECT sql FROM sqlite_master WHERE type='index' AND name='health_events_workout_hr_id'
            """)?.lowercased().filter { !$0.isWhitespace }
        guard columns == ["event_id", "value"],
              definition?.contains("whereoperation='upsert'andtype_identifier='boaz.workout.heart_rate'") == true else {
            throw LocalDatabaseError.incompatibleSchema("Workout heart-rate index has an incompatible layout.")
        }
    }

    private static func validateSchema(_ db: Database, expectedVersion: Int) throws {
        let required: [String: [String]] = [
            "health_events": ["event_id:TEXT:1", "revision:INTEGER:0", "operation:TEXT:0", "kind:TEXT:0", "type_identifier:TEXT:0", "start_utc:TEXT:0", "end_utc:TEXT:0", "value:REAL:0", "unit:TEXT:0", "payload:BLOB:0", "content_hash:TEXT:0", "updated_at:TEXT:0"],
            "query_anchors": ["type_identifier:TEXT:1", "anchor:BLOB:0", "updated_at:TEXT:0"],
            "upload_outbox": ["event_id:TEXT:1", "revision:INTEGER:2", "state:TEXT:0", "batch_id:TEXT:0", "attempts:INTEGER:0", "next_attempt_at:TEXT:0", "last_error:TEXT:0", "updated_at:TEXT:0"],
            "upload_batches": ["batch_id:TEXT:1", "body:BLOB:0", "body_hash:TEXT:0", "state:TEXT:0", "receipt:BLOB:0", "attempts:INTEGER:0", "next_attempt_at:TEXT:0", "last_error:TEXT:0", "created_at:TEXT:0", "updated_at:TEXT:0"],
            "sync_audit_log": ["id:INTEGER:1", "phase:TEXT:0", "outcome:TEXT:0", "detail:TEXT:0", "event_count:INTEGER:0", "created_at:TEXT:0"],
            "workout_detail_jobs": ["workout_id:TEXT:1", "event_offset:INTEGER:0", "events_done:INTEGER:0", "heart_rate_done:INTEGER:0", "updated_at:TEXT:0"],
            "local_control_state": ["key:TEXT:1", "value:TEXT:0"]
        ]
        for (table, expected) in required {
            let actual = try rows(db, "PRAGMA table_info(\"\(table)\")").map { row -> String in
                let name: String = row["name"]
                let type: String = row["type"]
                let pk: Int = row["pk"]
                return "\(name):\(type.uppercased()):\(pk)"
            }
            guard actual == expected else {
                throw LocalDatabaseError.incompatibleSchema("Table \(table) does not match the supported layout.")
            }
        }
        let indexes = Set(try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type='index'"))
        guard indexes.contains("health_events_type_time"), indexes.contains("upload_outbox_state") else {
            throw LocalDatabaseError.incompatibleSchema("A required index is missing.")
        }
        if expectedVersion == schemaVersion {
            let clock = try rows(db, "PRAGMA table_info(\"local_change_clock\")").map { row -> String in
                let name: String = row["name"]
                let type: String = row["type"]
                let pk: Int = row["pk"]
                return "\(name):\(type.uppercased()):\(pk)"
            }
            guard clock == ["id:INTEGER:1", "generation:INTEGER:0"],
                  try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM local_change_clock WHERE id=1") == 1 else {
                throw LocalDatabaseError.incompatibleSchema("The v2 change clock is missing or invalid.")
            }
            let triggers = Set(try String.fetchAll(db, sql: "SELECT name FROM sqlite_master WHERE type='trigger'"))
            guard triggers.isSuperset(of: ["health_events_clock_insert", "health_events_clock_update",
                                           "health_events_clock_delete"]) else {
                throw LocalDatabaseError.incompatibleSchema("A v2 change-clock trigger is missing.")
            }
        }
    }

    private static func protectFiles(around url: URL) throws {
        #if os(iOS)
        for path in [url.path, url.path + "-wal", url.path + "-shm"] where FileManager.default.fileExists(atPath: path) {
            try FileManager.default.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication], ofItemAtPath: path)
            #if !targetEnvironment(simulator)
            let attribute = try FileManager.default.attributesOfItem(atPath: path)[.protectionKey]
            let actual = (attribute as? FileProtectionType)?.rawValue ?? attribute as? String
            guard actual == FileProtectionType.completeUntilFirstUserAuthentication.rawValue else {
                throw LocalDatabaseError.open("File protection was not effective for \(URL(fileURLWithPath: path).lastPathComponent) (observed \(actual ?? "unavailable")).")
            }
            #endif
        }
        #endif
    }

    private static func encoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }

    private static func decoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }

    private static func decodeEvent(_ row: Row) throws -> HealthEvent {
        let payload: Data = row["payload"]
        return try decoder().decode(HealthEvent.self, from: payload)
    }

    private static func hash(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private static func timestamp(_ date: Date) -> String {
        ISO8601DateFormatter().string(from: date)
    }

    private static func parseDate(_ text: String) -> Date? {
        ISO8601DateFormatter().date(from: text)
    }

    private static func wire(event: HealthEvent) -> [String: Any] {
        let source: Any = event.operation == "delete" ? NSNull() : ["bundle_id": event.sourceBundleID, "name": event.sourceName]
        return [
            "event_id": event.eventID,
            "revision": event.revision,
            "operation": event.operation,
            "kind": event.kind,
            "type": event.type,
            "source": source,
            "start_utc": event.startUTC.map(timestamp) ?? NSNull(),
            "end_utc": event.endUTC.map(timestamp) ?? NSNull(),
            "value": event.value.map { $0 as Any } ?? NSNull(),
            "unit": event.unit.map { $0 as Any } ?? NSNull(),
            "metadata": event.metadata
        ]
    }

    /// Pages the complete current history, including records before 1970,
    /// without OFFSET or a total-record cap. The ID breaks timestamp ties.
    func historyPage(typeIdentifier: String, after cursor: HealthHistoryCursor? = nil,
                     limit: Int = 500, expectedVersion: HealthHistoryVersion? = nil) throws -> HealthHistoryPage {
        try Task.checkCancellation()
        guard (1...500).contains(limit) else {
            throw LocalDatabaseError.query("History page limit must be 1...500.")
        }
        return try pool.read { db in
            let version = try Self.historyVersion(db)
            if let expectedVersion { try Self.validateHistoryVersion(db, expectedVersion) }
            let tail = cursor == nil ? "" : " AND (start_utc,event_id) > (?,?)"
            var values: [BindValue] = [.text(typeIdentifier)]
            if let cursor { values += [.text(cursor.startUTC), .text(cursor.eventID)] }
            values.append(.int(limit))
            let rows = try Self.rows(db,
                "SELECT start_utc,event_id,payload FROM health_events WHERE operation='upsert' AND type_identifier=? AND start_utc IS NOT NULL"
                    + tail + " ORDER BY start_utc ASC,event_id ASC LIMIT ?", values)
            let events = try rows.map(Self.decodeEvent)
            let last: HealthHistoryCursor? = rows.last.map { row in
                let start: String = row["start_utc"]
                let id: String = row["event_id"]
                return HealthHistoryCursor(startUTC: start, eventID: id)
            }
            return HealthHistoryPage(events: events, nextCursor: events.count == limit ? last : nil,
                                     version: version)
        }
    }

    /// A complete stable scan is required before deleting old derived sleep
    /// values. The final version check and all changes share one writer txn.
    func reconcileDerivedSleep(_ current: [HealthEvent], expectedVersion: HealthHistoryVersion) async throws -> Int {
        let currentIDs = Set(current.map(\.eventID))
        var changes = current
        var cursor: HealthHistoryCursor?
        repeat {
            let page = try historyPage(typeIdentifier: SleepDerivation.derivedType, after: cursor,
                                       expectedVersion: expectedVersion)
            for old in page.events where !currentIDs.contains(old.eventID) {
                changes.append(HealthEvent(eventID: old.eventID, revision: 0, operation: "delete", kind: "quantity",
                                           type: old.type, sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                                           value: nil, unit: nil, metadata: ["derivation_version": "1"]))
            }
            cursor = page.nextCursor
        } while cursor != nil
        try Task.checkCancellation()
        let finalChanges = changes
        let result = try await pool.write { db in
            try Self.validateHistoryVersion(db, expectedVersion)
            return try Self.apply(db, events: finalChanges, anchor: nil, typeIdentifier: nil,
                                  expectedHistoryVersion: expectedVersion)
        }
        try verifyProtectionAfterCommit()
        return result
    }

    private static func historyVersion(_ db: Database) throws -> HealthHistoryVersion {
        guard let generation = try Int64.fetchOne(db, sql: "SELECT generation FROM local_change_clock WHERE id=1") else {
            throw LocalDatabaseError.incompatibleSchema("The local change clock is missing.")
        }
        return HealthHistoryVersion(generation: generation)
    }

    private static func validateHistoryVersion(_ db: Database, _ expected: HealthHistoryVersion) throws {
        guard try historyVersion(db) == expected else { throw HealthHistoryError.changedDuringRead }
    }
}

struct HealthHistoryCursor: Sendable, Equatable {
    let startUTC: String
    let eventID: String
}

struct HealthHistoryVersion: Sendable, Equatable {
    let generation: Int64
}

struct HealthHistoryPage: Sendable {
    let events: [HealthEvent]
    let nextCursor: HealthHistoryCursor?
    let version: HealthHistoryVersion
}

enum HealthHistoryError: Error, LocalizedError {
    case changedDuringRead
    case unorderedHistory

    var errorDescription: String? {
        switch self {
        case .changedDuringRead: "Health history changed during calculation. Sleep summaries will retry on the next collection."
        case .unorderedHistory: "Sleep history was not ordered. Previous summaries have been retained."
        }
    }
}
