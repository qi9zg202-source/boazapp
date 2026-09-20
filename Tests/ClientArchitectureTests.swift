import Foundation
import HealthKit
import XCTest
@testable import boazapp

final class ClientArchitectureTests: XCTestCase {
    func testCompletedErasureRotatesDeviceIdentityOnceAndNeverRollsBack() throws {
        let suiteName = "BoazConfigurationTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let original = BoazConfiguration.deviceID(in: defaults)
        let first = try BoazConfiguration.rotateDeviceID(afterCompletedErasure: "erasure-one", in: defaults)
        XCTAssertNotEqual(first, original)
        XCTAssertEqual(
            try BoazConfiguration.rotateDeviceID(afterCompletedErasure: "erasure-one", in: defaults),
            first
        )

        let second = try BoazConfiguration.rotateDeviceID(afterCompletedErasure: "erasure-two", in: defaults)
        XCTAssertNotEqual(second, first)
        XCTAssertEqual(
            try BoazConfiguration.rotateDeviceID(afterCompletedErasure: "erasure-one", in: defaults),
            second,
            "A replayed older completion must not restore an obsolete cloud identity"
        )
    }

    func testCancelledCollectionNeverAllowsUploadOrReportsFinish() {
        let cancelled = CollectionReport(savedEvents: 5, failedSources: [],
                                         storageProtectionUnverified: false, cancelled: true)
        XCTAssertFalse(cancelled.allowsUpload)
        XCTAssertNotEqual(SyncPhase.cancelled, .finished)
        XCTAssertFalse(SyncPhase.cancelled.isWorking)

        let unprotected = CollectionReport(savedEvents: 5, failedSources: [],
                                           storageProtectionUnverified: true, cancelled: false)
        XCTAssertFalse(unprotected.allowsUpload)

        let complete = CollectionReport(savedEvents: 5, failedSources: [],
                                        storageProtectionUnverified: false, cancelled: false)
        XCTAssertTrue(complete.allowsUpload)
    }

    func testCancelledPhaseTakesPrecedenceOverPartialSourceFailures() {
        let partial = CollectionReport(savedEvents: 2, failedSources: ["synthetic source"],
                                       storageProtectionUnverified: false, cancelled: false)
        XCTAssertEqual(SyncRunPresentation.phase(
            collection: partial, uploadFailed: false, uploadCancelled: true, taskCancelled: false
        ), .cancelled)
        XCTAssertEqual(SyncRunPresentation.phase(
            collection: partial, uploadFailed: false, uploadCancelled: false, taskCancelled: true
        ), .cancelled)
        XCTAssertEqual(SyncRunPresentation.phase(
            collection: partial, uploadFailed: false, uploadCancelled: false, taskCancelled: false
        ), .failed)

        let cancelledCollection = CollectionReport(savedEvents: 2, failedSources: ["synthetic source"],
                                                   storageProtectionUnverified: false, cancelled: true)
        XCTAssertEqual(SyncRunPresentation.phase(
            collection: cancelledCollection, uploadFailed: false, uploadCancelled: false, taskCancelled: false
        ), .cancelled)

        let unprotected = CollectionReport(savedEvents: 2, failedSources: [],
                                           storageProtectionUnverified: true, cancelled: true)
        XCTAssertEqual(SyncRunPresentation.phase(
            collection: unprotected, uploadFailed: false, uploadCancelled: false, taskCancelled: true
        ), .failed)
    }

    func testCancelledUploadCloudLabelFollowsLedgerEvidence() {
        let empty = DashboardSnapshot.empty
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: LocalHealthCounts(records: 0, pending: 0, cloudSaved: 0, metricsCurrent: 0),
            lastReceipt: nil, snapshot: empty
        ), .localOnly)
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: LocalHealthCounts(records: 3, pending: 0, cloudSaved: 0, metricsCurrent: 0),
            lastReceipt: nil, snapshot: empty
        ), .localSaved)
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: LocalHealthCounts(records: 3, pending: 2, cloudSaved: 0, metricsCurrent: 0),
            lastReceipt: nil, snapshot: empty
        ), .queued)
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: LocalHealthCounts(records: 3, pending: 0, cloudSaved: 2, metricsCurrent: 0),
            lastReceipt: nil, snapshot: empty
        ), .metricsPending)
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: LocalHealthCounts(records: 3, pending: 0, cloudSaved: 0, metricsCurrent: 3),
            lastReceipt: nil, snapshot: empty
        ), .metricsCurrent)
        XCTAssertEqual(SyncRunPresentation.cancelledUploadState(
            counts: nil, lastReceipt: nil, snapshot: empty
        ), .localOnly)
    }

    func testHealthQueryGateCancellationResumesEvenWithoutCallback() async {
        let gate = HealthQueryGate<Int>(store: HKHealthStore())
        let registered = expectation(description: "synthetic waiter registered")
        let waiting = Task<Int, Error> {
            try await withCheckedThrowingContinuation { continuation in
                gate.register(continuation)
                registered.fulfill()
            }
        }

        await fulfillment(of: [registered], timeout: 2)
        // Stopping a HealthKit query does not promise a callback. A callback
        // racing just after cancellation must not resume the waiter twice.
        gate.cancel()
        gate.finish(.success(42))
        do {
            _ = try await waiting.value
            XCTFail("Cancellation must resume the waiter with CancellationError")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testHealthQueryGateCancellationBeforeRegistrationDoesNotHang() async {
        let gate = HealthQueryGate<Int>(store: HKHealthStore())
        gate.cancel()
        do {
            _ = try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Int, Error>) in
                gate.register(continuation)
            }
            XCTFail("A canceled query must not start or wait for a callback")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testHealthQueryGateCancellationAfterInstallIgnoresDuplicateCallback() async {
        let gate = HealthQueryGate<Int>(store: HKHealthStore())
        let registered = expectation(description: "query waiter registered")
        let waiting = Task<Int, Error> {
            try await withCheckedThrowingContinuation { continuation in
                gate.register(continuation)
                registered.fulfill()
            }
        }
        await fulfillment(of: [registered], timeout: 2)
        let query = HKSampleQuery(sampleType: HKObjectType.workoutType(), predicate: nil, limit: 1, sortDescriptors: nil) {
            _, _, _ in
        }
        XCTAssertTrue(gate.install(query))
        gate.cancel()
        gate.finish(.success(12))
        gate.finish(.success(13))
        XCTAssertFalse(gate.install(query))
        do {
            _ = try await waiting.value
            XCTFail("A canceled installed query must not accept later callbacks")
        } catch is CancellationError {
            // Expected.
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    func testPairingAndErasureBodiesUseExplicitCodableKeys() throws {
        let uppercaseCode = String(repeating: "A1", count: 32)
        XCTAssertTrue(TokyoCredentialFormat.isHex64(uppercaseCode))
        XCTAssertEqual(TokyoCredentialFormat.normalizedHex64(" \(uppercaseCode)\n"), uppercaseCode.lowercased())
        XCTAssertFalse(TokyoCredentialFormat.isHex64(String(repeating: "a", count: 63)))
        XCTAssertFalse(TokyoCredentialFormat.isHex64(String(repeating: "g", count: 64)))

        let pairing = try JSONEncoder().encode(TokyoPairingRequest(code: "synthetic-code", deviceID: "synthetic-device"))
        let pairingObject = try XCTUnwrap(JSONSerialization.jsonObject(with: pairing) as? [String: String])
        XCTAssertEqual(pairingObject, ["code": "synthetic-code", "device_id": "synthetic-device"])

        let erasure = try JSONEncoder().encode(TokyoEraseRequest(
            confirmation: "ERASE_CLOUD_HEALTH_DATA", erasureID: "synthetic-id", erasureSecret: "synthetic-secret"
        ))
        let erasureObject = try XCTUnwrap(JSONSerialization.jsonObject(with: erasure) as? [String: String])
        XCTAssertEqual(erasureObject, [
            "confirmation": "ERASE_CLOUD_HEALTH_DATA",
            "erasure_id": "synthetic-id",
            "erasure_secret": "synthetic-secret"
        ])
    }
}
