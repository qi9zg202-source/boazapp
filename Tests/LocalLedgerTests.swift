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
        let ledger = try database()
        let type = "HKQuantityTypeIdentifierHeartRate"
        let anchor = Data([1, 2, 3])
        do {
            _ = try await ledger.apply(events: [event("good"), event("bad", value: .nan)], anchor: anchor, typeIdentifier: type)
            XCTFail("Non-finite HealthKit values must fail the entire page")
        } catch {}
        let failedAnchor = try await ledger.anchor(for: type)
        let failedCounts = try await ledger.counts()
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
}
