import CryptoKit
import Foundation
import SQLite3

enum LocalDatabaseError: Error, LocalizedError {
    case open(String)
    case query(String)
    case missingSchema
    case oversizedEvent

    var errorDescription: String? {
        switch self {
        case .open(let detail), .query(let detail): detail
        case .missingSchema: "The local database schema is missing from the app."
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

actor BoazLocalDatabase {
    nonisolated(unsafe) private let database: OpaquePointer
    private let url: URL
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    init(url: URL? = nil, schemaURL: URL? = nil) throws {
        let target: URL
        if let url {
            target = url
        } else {
            let directory = try FileManager.default.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            ).appendingPathComponent("BoazHealth", isDirectory: true)
            target = directory.appendingPathComponent("health.sqlite")
        }
        try FileManager.default.createDirectory(at: target.deletingLastPathComponent(), withIntermediateDirectories: true)
        #if os(iOS)
        try FileManager.default.setAttributes([.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication], ofItemAtPath: target.deletingLastPathComponent().path)
        #endif
        var pointer: OpaquePointer?
        let code = sqlite3_open_v2(target.path, &pointer, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil)
        guard code == SQLITE_OK, let pointer else {
            let message = pointer.map { String(cString: sqlite3_errmsg($0)) } ?? "unknown SQLite error"
            if let pointer { sqlite3_close(pointer) }
            throw LocalDatabaseError.open(message)
        }
        database = pointer
        self.url = target
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        self.decoder = decoder
        do {
            try Self.execute(pointer, sql: "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
            guard let selectedSchemaURL = schemaURL ?? Bundle.main.url(forResource: "Schema", withExtension: "sql"),
                  let schema = try? String(contentsOf: selectedSchemaURL, encoding: .utf8) else {
                throw LocalDatabaseError.missingSchema
            }
            try Self.execute(pointer, sql: schema)
            try Self.protectFiles(around: target)
        } catch {
            sqlite3_close(pointer)
            throw error
        }
    }

    deinit { sqlite3_close(database) }

    func anchor(for typeIdentifier: String) throws -> Data? {
        try statement("SELECT anchor FROM query_anchors WHERE type_identifier=?", values: [.text(typeIdentifier)]) { stmt in
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            let length = Int(sqlite3_column_bytes(stmt, 0))
            guard let bytes = sqlite3_column_blob(stmt, 0) else { return Data() }
            return Data(bytes: bytes, count: length)
        }
    }

    @discardableResult
    func apply(events: [HealthEvent], anchor: Data? = nil, typeIdentifier: String? = nil) throws -> Int {
        try transaction {
            var changed = 0
            for event in events {
                let incoming = try encoder.encode(event)
                let contentHash = Self.hash(incoming)
                let previous: (Int, String)? = try statement(
                    "SELECT revision, content_hash FROM health_events WHERE event_id=?",
                    values: [.text(event.eventID)]
                ) { stmt in
                    guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
                    return (Int(sqlite3_column_int64(stmt, 0)), Self.columnText(stmt, 1))
                }
                if previous?.1 == contentHash { continue }
                let revision = max(event.revision, (previous?.0 ?? 0) + 1, 1)
                var object = try JSONSerialization.jsonObject(with: incoming) as? [String: Any] ?? [:]
                object["revision"] = revision
                let stored = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
                let now = Self.timestamp(Date())
                try statement(
                    """
                    INSERT INTO health_events(event_id,revision,operation,kind,type_identifier,start_utc,end_utc,value,unit,payload,content_hash,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(event_id) DO UPDATE SET revision=excluded.revision,operation=excluded.operation,kind=excluded.kind,
                    type_identifier=excluded.type_identifier,start_utc=excluded.start_utc,end_utc=excluded.end_utc,value=excluded.value,
                    unit=excluded.unit,payload=excluded.payload,content_hash=excluded.content_hash,updated_at=excluded.updated_at
                    """,
                    values: [
                        .text(event.eventID), .int(revision), .text(event.operation), .text(event.kind), .text(event.type),
                        event.startUTC.map { .text(Self.timestamp($0)) } ?? .null,
                        event.endUTC.map { .text(Self.timestamp($0)) } ?? .null,
                        event.value.map { .double($0) } ?? .null,
                        event.unit.map { .text($0) } ?? .null,
                        .blob(stored), .text(contentHash), .text(now)
                    ]
                ) { stmt in try stepDone(stmt) }
                try statement("DELETE FROM upload_outbox WHERE event_id=?", values: [.text(event.eventID)]) { stmt in try stepDone(stmt) }
                try statement(
                    "INSERT INTO upload_outbox(event_id,revision,state,updated_at) VALUES(?,?,'pending',?)",
                    values: [.text(event.eventID), .int(revision), .text(now)]
                ) { stmt in try stepDone(stmt) }
                if event.kind == "workout", UUID(uuidString: event.eventID) != nil {
                    changed += try tombstoneWorkoutDetails(id: event.eventID)
                    try statement("DELETE FROM query_anchors WHERE type_identifier=?", values: [.text("workout-heart-rate:\(event.eventID)")]) { stmt in try stepDone(stmt) }
                    if event.operation == "upsert" {
                        try statement(
                            """
                            INSERT INTO workout_detail_jobs(workout_id,event_offset,events_done,heart_rate_done,updated_at)
                            VALUES(?,0,0,0,?) ON CONFLICT(workout_id) DO UPDATE SET
                            event_offset=0,events_done=0,heart_rate_done=0,updated_at=excluded.updated_at
                            """,
                            values: [.text(event.eventID), .text(now)]
                        ) { stmt in try stepDone(stmt) }
                    } else {
                        try statement("DELETE FROM workout_detail_jobs WHERE workout_id=?", values: [.text(event.eventID)]) { stmt in try stepDone(stmt) }
                    }
                }
                changed += 1
            }
            if let anchor, let typeIdentifier {
                try statement(
                    """
                    INSERT INTO query_anchors(type_identifier,anchor,updated_at) VALUES(?,?,?)
                    ON CONFLICT(type_identifier) DO UPDATE SET anchor=excluded.anchor,updated_at=excluded.updated_at
                    """,
                    values: [.text(typeIdentifier), .blob(anchor), .text(Self.timestamp(Date()))]
                ) { stmt in try stepDone(stmt) }
            }
            try writeAudit(phase: "collect", outcome: "local_saved", detail: typeIdentifier ?? "activity", count: changed)
            return changed
        }
    }

    func prepareBatch(deviceID: String) throws -> PreparedHealthBatch? {
        if let prior: (PreparedHealthBatch, String?) = try statement(
            "SELECT batch_id,body,body_hash,next_attempt_at FROM upload_batches WHERE state='pending' ORDER BY created_at LIMIT 1",
            values: [],
            body: { stmt in
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            let count = Int(sqlite3_column_bytes(stmt, 1))
            guard let bytes = sqlite3_column_blob(stmt, 1) else { return nil }
            let body = Data(bytes: bytes, count: count)
            let json = (try? JSONSerialization.jsonObject(with: body) as? [String: Any]) ?? [:]
            let retryAt = sqlite3_column_type(stmt, 3) == SQLITE_NULL ? nil : Self.columnText(stmt, 3)
            return (PreparedHealthBatch(id: Self.columnText(stmt, 0), body: body, contentHash: Self.columnText(stmt, 2), eventCount: (json["events"] as? [[String: Any]])?.count ?? 0), retryAt)
        }) {
            if let retryAt = prior.1, retryAt > Self.timestamp(Date()) { return nil }
            return prior.0
        }

        let rows: [(String, Int, Data)] = try statement(
            """
            SELECT o.event_id,o.revision,e.payload FROM upload_outbox o JOIN health_events e ON e.event_id=o.event_id
            WHERE o.state='pending' AND o.batch_id IS NULL ORDER BY e.updated_at,o.event_id LIMIT 200
            """,
            values: []
        ) { stmt in
            var rows: [(String, Int, Data)] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                let count = Int(sqlite3_column_bytes(stmt, 2))
                guard let bytes = sqlite3_column_blob(stmt, 2) else { continue }
                rows.append((Self.columnText(stmt, 0), Int(sqlite3_column_int64(stmt, 1)), Data(bytes: bytes, count: count)))
            }
            return rows
        }
        guard !rows.isEmpty else { return nil }
        let id = UUID().uuidString.lowercased()
        var selected: [(String, Int, [String: Any])] = []
        var body = Data()
        for row in rows {
            let event = try decoder.decode(HealthEvent.self, from: row.2)
            let wire = Self.wire(event: event)
            let candidate = selected + [(row.0, row.1, wire)]
            let object: [String: Any] = ["schema_version": 1, "device_id": deviceID, "batch_id": id, "events": candidate.map { $0.2 }]
            let data = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
            if data.count > 128 * 1024 {
                if selected.isEmpty { throw LocalDatabaseError.oversizedEvent }
                break
            }
            selected = candidate
            body = data
        }
        let hash = Self.hash(body)
        let selectedKeys = selected.map { ($0.0, $0.1) }
        try transaction {
            let now = Self.timestamp(Date())
            try statement(
                "INSERT INTO upload_batches(batch_id,body,body_hash,state,created_at,updated_at) VALUES(?,?,?,'pending',?,?)",
                values: [.text(id), .blob(body), .text(hash), .text(now), .text(now)]
            ) { stmt in try stepDone(stmt) }
            for row in selectedKeys {
                try statement("UPDATE upload_outbox SET batch_id=?,state='sending',updated_at=? WHERE event_id=? AND revision=?", values: [.text(id), .text(now), .text(row.0), .int(row.1)]) { stmt in try stepDone(stmt) }
            }
        }
        return PreparedHealthBatch(id: id, body: body, contentHash: hash, eventCount: selected.count)
    }

    func markCloudSaved(batchID: String, receipt: Data) throws {
        try transaction {
            let now = Self.timestamp(Date())
            let accepted = (try? decoder.decode(TokyoReceipt.self, from: receipt).acceptedEvents) ?? 0
            try statement("UPDATE upload_batches SET state='cloud_saved',receipt=?,updated_at=? WHERE batch_id=?", values: [.blob(receipt), .text(now), .text(batchID)]) { stmt in try stepDone(stmt) }
            try statement("UPDATE upload_outbox SET state='cloud_saved',updated_at=? WHERE batch_id=?", values: [.text(now), .text(batchID)]) { stmt in try stepDone(stmt) }
            try writeAudit(phase: "upload", outcome: "cloud_saved", detail: batchID, count: accepted)
        }
    }

    func markMetricsCurrent(batchID: String) throws {
        try transaction {
            let now = Self.timestamp(Date())
            try statement("UPDATE upload_batches SET state='metrics_current',updated_at=? WHERE batch_id=?", values: [.text(now), .text(batchID)]) { stmt in try stepDone(stmt) }
            try statement("UPDATE upload_outbox SET state='metrics_current',updated_at=? WHERE batch_id=?", values: [.text(now), .text(batchID)]) { stmt in try stepDone(stmt) }
            try writeAudit(phase: "project", outcome: "metrics_current", detail: batchID, count: 0)
        }
    }

    /// An erasure resets cloud receipts while retaining the local HealthKit snapshot.
    /// The marker makes recovery after a lost erase response safe to repeat.
    func requeueAfterErasure(id: String) throws {
        try transaction {
            let prior: String? = try statement("SELECT value FROM local_control_state WHERE key='last_erasure_requeue'", values: []) { stmt in
                guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
                return Self.columnText(stmt, 0)
            }
            guard prior != id else { return }
            try Self.execute(database, sql: "DELETE FROM upload_outbox; DELETE FROM upload_batches;")
            let now = Self.timestamp(Date())
            try statement(
                "INSERT INTO upload_outbox(event_id,revision,state,updated_at) SELECT event_id,revision,'pending',? FROM health_events",
                values: [.text(now)]
            ) { stmt in try stepDone(stmt) }
            try statement(
                "INSERT INTO local_control_state(key,value) VALUES('last_erasure_requeue',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                values: [.text(id)]
            ) { stmt in try stepDone(stmt) }
            try writeAudit(phase: "erase", outcome: "local_requeued", detail: id, count: 0)
        }
    }

    func recordErasureStatus(id: String, status: String) throws {
        try transaction {
            let value = "\(id):\(status)"
            let prior: String? = try statement("SELECT value FROM local_control_state WHERE key='last_erasure_status'", values: []) { stmt in
                guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
                return Self.columnText(stmt, 0)
            }
            guard prior != value else { return }
            try statement(
                "INSERT INTO local_control_state(key,value) VALUES('last_erasure_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                values: [.text(value)]
            ) { stmt in try stepDone(stmt) }
            try writeAudit(phase: "erase", outcome: status, detail: id, count: 0)
        }
    }

    func lastErasureCompleted() throws -> Bool {
        let value: String? = try statement("SELECT value FROM local_control_state WHERE key='last_erasure_status'", values: []) { stmt in
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            return Self.columnText(stmt, 0)
        }
        return value?.hasSuffix(":complete") == true
    }

    func deferBatch(batchID: String, error: String) throws {
        try transaction {
            let attempts: Int = try statement("SELECT attempts FROM upload_batches WHERE batch_id=?", values: [.text(batchID)]) { stmt in
                guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
                return Int(sqlite3_column_int64(stmt, 0))
            }
            let next = min(3600, Int(pow(2.0, Double(min(attempts + 1, 11))))) + Int.random(in: 0...3)
            try statement(
                "UPDATE upload_batches SET attempts=attempts+1,next_attempt_at=?,last_error=?,updated_at=? WHERE batch_id=?",
                values: [.text(Self.timestamp(Date().addingTimeInterval(Double(next)))), .text(String(error.prefix(300))), .text(Self.timestamp(Date())), .text(batchID)]
            ) { stmt in try stepDone(stmt) }
            try writeAudit(phase: "upload", outcome: "retry_queued", detail: batchID, count: 0)
        }
    }

    func cloudSavedBatchIDs() throws -> [String] {
        try statement("SELECT batch_id FROM upload_batches WHERE state='cloud_saved' ORDER BY created_at LIMIT 20", values: []) { stmt in
            var ids: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW { ids.append(Self.columnText(stmt, 0)) }
            return ids
        }
    }

    func recordFailure(phase: String, detail: String) throws {
        try writeAudit(phase: phase, outcome: "failed", detail: String(detail.prefix(300)), count: 0)
    }

    func counts() throws -> LocalHealthCounts {
        func scalar(_ sql: String) throws -> Int {
            try statement(sql, values: []) { stmt in
                guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
                return Int(sqlite3_column_int64(stmt, 0))
            }
        }
        return try LocalHealthCounts(
            records: scalar("SELECT COUNT(*) FROM health_events WHERE operation='upsert'"),
            pending: scalar("SELECT COUNT(*) FROM upload_outbox WHERE state IN ('pending','sending')"),
            cloudSaved: scalar("SELECT COUNT(*) FROM upload_outbox WHERE state='cloud_saved'"),
            metricsCurrent: scalar("SELECT COUNT(*) FROM upload_outbox WHERE state='metrics_current'")
        )
    }

    func recentEvents(since: Date, limit: Int = 10_000) throws -> [HealthEvent] {
        try statement(
            "SELECT payload FROM health_events WHERE operation='upsert' AND start_utc>=? ORDER BY start_utc DESC LIMIT ?",
            values: [.text(Self.timestamp(since)), .int(limit)]
        ) { stmt in
            var events: [HealthEvent] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                let count = Int(sqlite3_column_bytes(stmt, 0))
                if let bytes = sqlite3_column_blob(stmt, 0) {
                    events.append(try decoder.decode(HealthEvent.self, from: Data(bytes: bytes, count: count)))
                }
            }
            return events
        }
    }

    func latestEvent(typeIdentifier: String) throws -> HealthEvent? {
        try statement(
            "SELECT payload FROM health_events WHERE operation='upsert' AND type_identifier=? ORDER BY start_utc DESC LIMIT 1",
            values: [.text(typeIdentifier)]
        ) { stmt in
            guard sqlite3_step(stmt) == SQLITE_ROW, let bytes = sqlite3_column_blob(stmt, 0) else { return nil }
            return try decoder.decode(HealthEvent.self, from: Data(bytes: bytes, count: Int(sqlite3_column_bytes(stmt, 0))))
        }
    }

    func events(typeIdentifier: String, since: Date, limit: Int = 10_000) throws -> [HealthEvent] {
        try statement(
            "SELECT payload FROM health_events WHERE operation='upsert' AND type_identifier=? AND start_utc>=? ORDER BY start_utc DESC LIMIT ?",
            values: [.text(typeIdentifier), .text(Self.timestamp(since)), .int(limit)]
        ) { stmt in
            var events: [HealthEvent] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let bytes = sqlite3_column_blob(stmt, 0) {
                    events.append(try decoder.decode(HealthEvent.self, from: Data(bytes: bytes, count: Int(sqlite3_column_bytes(stmt, 0)))))
                }
            }
            return events
        }
    }

    func audit(limit: Int = 30) throws -> [LocalAuditEntry] {
        try statement("SELECT id,phase,outcome,detail,event_count,created_at FROM sync_audit_log ORDER BY id DESC LIMIT ?", values: [.int(limit)]) { stmt in
            var entries: [LocalAuditEntry] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                entries.append(LocalAuditEntry(
                    id: sqlite3_column_int64(stmt, 0), phase: Self.columnText(stmt, 1), outcome: Self.columnText(stmt, 2),
                    detail: Self.columnText(stmt, 3), eventCount: Int(sqlite3_column_int64(stmt, 4)),
                    createdAt: Self.parseDate(Self.columnText(stmt, 5)) ?? .distantPast
                ))
            }
            return entries
        }
    }

    func pendingWorkoutIDs(limit: Int = 100) throws -> [UUID] {
        try statement(
            "SELECT workout_id FROM workout_detail_jobs WHERE events_done=0 OR heart_rate_done=0 ORDER BY updated_at LIMIT ?",
            values: [.int(limit)]
        ) { stmt in
            var ids: [UUID] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                if let id = UUID(uuidString: Self.columnText(stmt, 0)) { ids.append(id) }
            }
            return ids
        }
    }

    func workoutProgress(id: UUID) throws -> (offset: Int, eventsDone: Bool, heartRateDone: Bool)? {
        try statement(
            "SELECT event_offset,events_done,heart_rate_done FROM workout_detail_jobs WHERE workout_id=?",
            values: [.text(id.uuidString)]
        ) { stmt in
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            return (Int(sqlite3_column_int64(stmt, 0)), sqlite3_column_int64(stmt, 1) != 0, sqlite3_column_int64(stmt, 2) != 0)
        }
    }

    func advanceWorkoutEvents(id: UUID, nextOffset: Int, done: Bool) throws {
        try statement(
            "UPDATE workout_detail_jobs SET event_offset=?,events_done=?,updated_at=? WHERE workout_id=?",
            values: [.int(nextOffset), .int(done ? 1 : 0), .text(Self.timestamp(Date())), .text(id.uuidString)]
        ) { stmt in try stepDone(stmt) }
    }

    func markWorkoutHeartRateDone(id: UUID) throws {
        try statement(
            "UPDATE workout_detail_jobs SET heart_rate_done=1,updated_at=? WHERE workout_id=?",
            values: [.text(Self.timestamp(Date())), .text(id.uuidString)]
        ) { stmt in try stepDone(stmt) }
    }

    private func writeAudit(phase: String, outcome: String, detail: String, count: Int) throws {
        try statement("INSERT INTO sync_audit_log(phase,outcome,detail,event_count,created_at) VALUES(?,?,?,?,?)", values: [.text(phase), .text(outcome), .text(detail), .int(count), .text(Self.timestamp(Date()))]) { stmt in try stepDone(stmt) }
    }

    private func tombstoneWorkoutDetails(id: String) throws -> Int {
        let children: [(String, Int, String, String)] = try statement(
            "SELECT event_id,revision,kind,type_identifier FROM health_events WHERE event_id LIKE ? AND operation='upsert'",
            values: [.text("workout:\(id):%")]
        ) { stmt in
            var rows: [(String, Int, String, String)] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                rows.append((Self.columnText(stmt, 0), Int(sqlite3_column_int64(stmt, 1)),
                             Self.columnText(stmt, 2), Self.columnText(stmt, 3)))
            }
            return rows
        }
        let now = Self.timestamp(Date())
        for child in children {
            let revision = child.1 + 1
            let deleted = HealthEvent(eventID: child.0, revision: revision, operation: "delete",
                                      kind: child.2, type: child.3, sourceBundleID: "", sourceName: "",
                                      startUTC: nil, endUTC: nil, value: nil, unit: nil,
                                      metadata: ["workout_id": id])
            let payload = try encoder.encode(deleted)
            try statement(
                """
                UPDATE health_events SET revision=?,operation='delete',start_utc=NULL,end_utc=NULL,value=NULL,
                unit=NULL,payload=?,content_hash=?,updated_at=? WHERE event_id=?
                """,
                values: [.int(revision), .blob(payload), .text(Self.hash(payload)), .text(now), .text(child.0)]
            ) { stmt in try stepDone(stmt) }
            try statement("DELETE FROM upload_outbox WHERE event_id=?", values: [.text(child.0)]) { stmt in try stepDone(stmt) }
            try statement(
                "INSERT INTO upload_outbox(event_id,revision,state,updated_at) VALUES(?,?,'pending',?)",
                values: [.text(child.0), .int(revision), .text(now)]
            ) { stmt in try stepDone(stmt) }
        }
        return children.count
    }

    private enum BindValue {
        case text(String), int(Int), double(Double), blob(Data), null
    }

    private func statement<T>(_ sql: String, values: [BindValue], body: (OpaquePointer) throws -> T) throws -> T {
        var pointer: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &pointer, nil) == SQLITE_OK, let pointer else {
            throw LocalDatabaseError.query(String(cString: sqlite3_errmsg(database)))
        }
        defer { sqlite3_finalize(pointer) }
        for (offset, value) in values.enumerated() {
            let index = Int32(offset + 1)
            let result: Int32
            switch value {
            case .text(let text):
                result = text.withCString { sqlite3_bind_text(pointer, index, $0, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self)) }
            case .int(let number): result = sqlite3_bind_int64(pointer, index, Int64(number))
            case .double(let number): result = sqlite3_bind_double(pointer, index, number)
            case .blob(let data):
                result = data.withUnsafeBytes { bytes in sqlite3_bind_blob(pointer, index, bytes.baseAddress, Int32(data.count), unsafeBitCast(-1, to: sqlite3_destructor_type.self)) }
            case .null: result = sqlite3_bind_null(pointer, index)
            }
            guard result == SQLITE_OK else { throw LocalDatabaseError.query(String(cString: sqlite3_errmsg(database))) }
        }
        return try body(pointer)
    }

    private func stepDone(_ statement: OpaquePointer) throws {
        guard sqlite3_step(statement) == SQLITE_DONE else { throw LocalDatabaseError.query(String(cString: sqlite3_errmsg(database))) }
    }

    private func transaction<T>(_ body: () throws -> T) throws -> T {
        try Self.execute(database, sql: "BEGIN IMMEDIATE")
        do {
            let result = try body()
            try Self.execute(database, sql: "COMMIT")
            try Self.protectFiles(around: url)
            return result
        } catch {
            try? Self.execute(database, sql: "ROLLBACK")
            throw error
        }
    }

    private static func execute(_ database: OpaquePointer, sql: String) throws {
        var error: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(database, sql, nil, nil, &error) == SQLITE_OK else {
            let detail = error.map { String(cString: $0) } ?? String(cString: sqlite3_errmsg(database))
            if let error { sqlite3_free(error) }
            throw LocalDatabaseError.query(detail)
        }
    }

    private static func protectFiles(around url: URL) throws {
        #if os(iOS)
        for path in [url.path, url.path + "-wal", url.path + "-shm"] where FileManager.default.fileExists(atPath: path) {
            try FileManager.default.setAttributes([.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication], ofItemAtPath: path)
        }
        #endif
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

    private static func columnText(_ stmt: OpaquePointer, _ index: Int32) -> String {
        guard let value = sqlite3_column_text(stmt, index) else { return "" }
        return String(cString: value)
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
}
