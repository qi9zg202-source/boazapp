import Foundation
import XCTest
@testable import boazapp

final class LocalLedgerTests: XCTestCase {
    private func database() throws -> BoazLocalDatabase {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        return try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
    }

    private func event(_ id: String, operation: String = "upsert", value: Double? = 72) -> HealthEvent {
        HealthEvent(
            eventID: id, revision: 0, operation: operation, kind: "quantity",
            type: "HKQuantityTypeIdentifierHeartRate", sourceBundleID: "test.watch", sourceName: "Test Watch",
            startUTC: operation == "upsert" ? Date(timeIntervalSince1970: 1_700_000_000) : nil,
            endUTC: operation == "upsert" ? Date(timeIntervalSince1970: 1_700_000_001) : nil,
            value: operation == "upsert" ? value : nil, unit: operation == "upsert" ? "count/min" : nil,
            metadata: [:]
        )
    }

    func testFailedPageDoesNotAdvanceAnchor() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let path = folder.appendingPathComponent("health.sqlite")
        let ledger = try BoazLocalDatabase(url: path)
        let type = "HKQuantityTypeIdentifierHeartRate"
        let anchor = Data([1, 2, 3])
        do {
            _ = try await ledger.apply(events: [event("good"), event("bad", value: .nan)], anchor: anchor, typeIdentifier: type)
            XCTFail("Non-finite HealthKit values must fail the entire page")
        } catch {}
        let reopened = try BoazLocalDatabase(url: path)
        let failedAnchor = try await reopened.anchor(for: type)
        let failedCounts = try await reopened.counts()
        XCTAssertNil(failedAnchor)
        XCTAssertEqual(failedCounts.records, 0)

        let firstCount = try await ledger.apply(events: [event("good")], anchor: anchor, typeIdentifier: type)
        let firstAnchor = try await ledger.anchor(for: type)
        let repeatedCount = try await ledger.apply(events: [event("good")], anchor: Data([4]), typeIdentifier: type)
        let repeatedAnchor = try await ledger.anchor(for: type)
        let repeatedCounts = try await ledger.counts()
        XCTAssertEqual(firstCount, 1)
        XCTAssertEqual(firstAnchor, anchor)
        XCTAssertEqual(repeatedCount, 0)
        XCTAssertEqual(repeatedAnchor, Data([4]))
        XCTAssertEqual(repeatedCounts.pending, 1)
    }

    func testBoundedBatchAndStableRetryBody() async throws {
        let ledger = try database()
        let samples = (0..<450).map { event("sample-\($0)") }
        let saved = try await ledger.apply(events: samples)
        XCTAssertEqual(saved, 450)
        let firstOptional = try await ledger.prepareBatch(deviceID: "test-device")
        let first = try XCTUnwrap(firstOptional)
        XCTAssertEqual(first.eventCount, 200)
        XCTAssertLessThanOrEqual(first.body.count, 128 * 1024)
        let retryOptional = try await ledger.prepareBatch(deviceID: "test-device")
        let retry = try XCTUnwrap(retryOptional)
        XCTAssertEqual(retry.id, first.id)
        XCTAssertEqual(retry.body, first.body)

        let receipt = TokyoReceipt(batchID: first.id, status: "cloud_saved", acceptedEvents: 200,
                                   receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                   contentHash: first.contentHash, commitSequence: 1)
        try await ledger.markCloudSaved(batchID: first.id, receipt: JSONEncoder().encode(receipt))
        let secondOptional = try await ledger.prepareBatch(deviceID: "test-device")
        let second = try XCTUnwrap(secondOptional)
        XCTAssertNotEqual(second.id, first.id)
        XCTAssertEqual(second.eventCount, 200)
        let remaining = try await ledger.counts()
        XCTAssertEqual(remaining.pending, 250)
    }

    func testNewDeletionSurvivesOldBatchReceipt() async throws {
        let ledger = try database()
        _ = try await ledger.apply(events: [event("one")])
        let oldOptional = try await ledger.prepareBatch(deviceID: "test-device")
        let old = try XCTUnwrap(oldOptional)
        _ = try await ledger.apply(events: [event("one", operation: "delete")])
        let receipt = TokyoReceipt(batchID: old.id, status: "cloud_saved", acceptedEvents: 1,
                                   receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                   contentHash: old.contentHash, commitSequence: 1)
        try await ledger.markCloudSaved(batchID: old.id, receipt: JSONEncoder().encode(receipt))
        let nextOptional = try await ledger.prepareBatch(deviceID: "test-device")
        let next = try XCTUnwrap(nextOptional)
        let body = try XCTUnwrap(JSONSerialization.jsonObject(with: next.body) as? [String: Any])
        let events = try XCTUnwrap(body["events"] as? [[String: Any]])
        XCTAssertEqual(events.first?["operation"] as? String, "delete")
        XCTAssertEqual(events.first?["revision"] as? Int, 2)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.pending, 1)
    }

    func testUnboundReceiptsCannotAdvanceLocalState() async throws {
        let ledger = try database()
        _ = try await ledger.apply(events: [event("receipt-sample")])
        let pending = try await ledger.prepareBatch(deviceID: "test-device")
        let batch = try XCTUnwrap(pending)
        let invalid = [
            TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 1, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: nil, commitSequence: 1),
            TokyoReceipt(batchID: "another-batch", status: "cloud_saved", acceptedEvents: 1, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: batch.contentHash, commitSequence: 1),
            TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 2, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: batch.contentHash, commitSequence: 1),
            TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 1, receivedAt: "invalid", projectedAt: nil, contentHash: batch.contentHash, commitSequence: 1),
            TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 1, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: batch.contentHash, commitSequence: 0),
            TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 1, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: batch.contentHash, commitSequence: nil),
            TokyoReceipt(batchID: batch.id, status: "invented_status", acceptedEvents: 1, receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil, contentHash: batch.contentHash, commitSequence: 1)
        ]
        for receipt in invalid {
            do {
                try await ledger.markCloudSaved(batchID: batch.id, receipt: JSONEncoder().encode(receipt))
                XCTFail("A receipt without complete matching evidence advanced local state")
            } catch {}
        }
        do {
            try await ledger.markCloudSaved(batchID: batch.id, receipt: Data("not-json".utf8))
            XCTFail("Malformed response advanced state")
        } catch {}
        let retry = try await ledger.prepareBatch(deviceID: "test-device")
        XCTAssertEqual(retry?.body, batch.body)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.pending, 1)
        XCTAssertEqual(counts.cloudSaved, 0)
        XCTAssertEqual(counts.metricsCurrent, 0)
    }

    func testMetricsConfirmationIsBoundToStoredBatch() async throws {
        let ledger = try database()
        _ = try await ledger.apply(events: [event("metric-sample")])
        let pending = try await ledger.prepareBatch(deviceID: "test-device")
        let batch = try XCTUnwrap(pending)
        let saved = TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: 1,
                                 receivedAt: "2026-09-18T00:00:00.123Z", projectedAt: nil,
                                 contentHash: batch.contentHash, commitSequence: 1)
        try await ledger.markCloudSaved(batchID: batch.id, receipt: JSONEncoder().encode(saved))
        let bad = TokyoReceipt(batchID: batch.id, status: "metrics_current", acceptedEvents: 1,
                               receivedAt: saved.receivedAt, projectedAt: "2026-09-18T00:01:00Z",
                               contentHash: "different-request", commitSequence: 1)
        do {
            try await ledger.markMetricsCurrent(batchID: batch.id, receipt: bad)
            XCTFail("Wrong projection receipt must be rejected")
        } catch {}
        let missingTime = TokyoReceipt(batchID: batch.id, status: "metrics_current", acceptedEvents: 1,
                                       receivedAt: saved.receivedAt, projectedAt: nil,
                                       contentHash: batch.contentHash, commitSequence: 1)
        do {
            try await ledger.markMetricsCurrent(batchID: batch.id, receipt: missingTime)
            XCTFail("Missing projection timestamp must be rejected")
        } catch {}
        let before = try await ledger.counts()
        XCTAssertEqual(before.cloudSaved, 1)
        XCTAssertEqual(before.metricsCurrent, 0)
        let good = TokyoReceipt(batchID: batch.id, status: "metrics_current", acceptedEvents: 1,
                                receivedAt: saved.receivedAt, projectedAt: "2026-09-18T00:01:00Z",
                                contentHash: batch.contentHash, commitSequence: 1)
        try await ledger.markMetricsCurrent(batchID: batch.id, receipt: good)
        let after = try await ledger.counts()
        XCTAssertEqual(after.cloudSaved, 0)
        XCTAssertEqual(after.metricsCurrent, 1)
    }

    func testActivityRevisionAndRetrySurviveDatabaseReopen() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let path = folder.appendingPathComponent("health.sqlite")
        let first = try BoazLocalDatabase(url: path)
        func day(_ value: Double) -> HealthEvent {
            HealthEvent(eventID: "activity:2026-09-18:stand", revision: 0, operation: "upsert", kind: "activity",
                        type: "activity.stand", sourceBundleID: "com.apple.health", sourceName: "Health",
                        startUTC: Date(timeIntervalSince1970: 1_700_000_000), endUTC: Date(timeIntervalSince1970: 1_700_086_400),
                        value: value, unit: "hours", metadata: ["timezone": "UTC"])
        }
        _ = try await first.apply(events: [day(8)])
        _ = try await first.apply(events: [day(9)])
        let pending = try await first.prepareBatch(deviceID: "test-device")
        let batch = try XCTUnwrap(pending)
        let reopened = try BoazLocalDatabase(url: path)
        let retry = try await reopened.prepareBatch(deviceID: "test-device")
        XCTAssertEqual(retry?.body, batch.body)
        let records = try await reopened.recentEvents(since: .distantPast)
        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records.first?.value, 9)
        XCTAssertEqual(records.first?.revision, 2)
    }

    func testHealthDirectoryIsExcludedFromSystemBackups() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let ledger = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        _ = try await ledger.apply(events: [event("backup-policy")])
        let values = try folder.resourceValues(forKeys: [.isExcludedFromBackupKey])
        XCTAssertEqual(values.isExcludedFromBackup, true)
    }

    func testByteBoundAndOversizedEventRetainTheQueue() async throws {
        func padded(_ id: String, count: Int) -> HealthEvent {
            let base = event(id)
            return HealthEvent(eventID: id, revision: 0, operation: base.operation, kind: base.kind, type: base.type,
                               sourceBundleID: base.sourceBundleID, sourceName: base.sourceName,
                               startUTC: base.startUTC, endUTC: base.endUTC, value: base.value, unit: base.unit,
                               metadata: ["synthetic_padding": String(repeating: "x", count: count)])
        }
        let ledger = try database()
        _ = try await ledger.apply(events: [padded("large-a", count: 70 * 1024), padded("large-b", count: 70 * 1024)])
        let pending = try await ledger.prepareBatch(deviceID: "test-device")
        let batch = try XCTUnwrap(pending)
        XCTAssertEqual(batch.eventCount, 1)
        XCTAssertLessThanOrEqual(batch.body.count, 128 * 1024)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.pending, 2)

        let oversized = try database()
        _ = try await oversized.apply(events: [padded("oversized", count: 129 * 1024)])
        do {
            _ = try await oversized.prepareBatch(deviceID: "test-device")
            XCTFail("Oversized event should remain local with an explicit error")
        } catch LocalDatabaseError.oversizedEvent {}
        let retained = try await oversized.counts()
        XCTAssertEqual(retained.pending, 1)
        XCTAssertEqual(retained.records, 1)
    }

    func testRetryBackoffSurvivesReopen() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let path = folder.appendingPathComponent("health.sqlite")
        let ledger = try BoazLocalDatabase(url: path)
        _ = try await ledger.apply(events: [event("retry")])
        let pending = try await ledger.prepareBatch(deviceID: "test-device")
        let batch = try XCTUnwrap(pending)
        try await ledger.deferBatch(batchID: batch.id, error: "Synthetic offline failure")
        let reopened = try BoazLocalDatabase(url: path)
        let immediate = try await reopened.prepareBatch(deviceID: "test-device")
        XCTAssertNil(immediate)
        let counts = try await reopened.counts()
        XCTAssertEqual(counts.pending, 1)
        let audit = try await reopened.audit()
        XCTAssertTrue(audit.contains { $0.outcome == "retry_queued" })
    }

    @MainActor
    func testUploadDoesNotStartWithoutSeparateConsent() async throws {
        let previousConsent = BoazConfiguration.uploadConsent
        BoazConfiguration.uploadConsent = false
        defer { BoazConfiguration.uploadConsent = previousConsent }
        let ledger = try database()
        _ = try await ledger.apply(events: [event("consent-gate")])
        let engine = SyncEngine(health: HealthKitManager(), database: ledger, gateway: TokyoCloudGateway())
        let result = await engine.uploadPending()
        XCTAssertEqual(result.committedEvents, 0)
        XCTAssertNil(result.lastReceipt)
        XCTAssertNil(result.failure)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.pending, 1)
        XCTAssertEqual(counts.cloudSaved, 0)
    }
}
