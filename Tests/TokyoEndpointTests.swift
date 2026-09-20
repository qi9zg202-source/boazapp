import Foundation
import XCTest
@testable import boazapp

private struct SyntheticTokyoResponse: Sendable {
    let status: Int
    let body: Data
}

private struct CapturedTokyoRequest: Sendable {
    let url: String
    let method: String
    let contentType: String?
    let idempotencyKey: String?
    let authorization: String?
    let body: Data
}

private final class SyntheticTokyoTransport: @unchecked Sendable {
    private let lock = NSLock()
    private var planned: [SyntheticTokyoResponse] = []
    private var captured: [CapturedTokyoRequest] = []

    func reset(_ responses: [SyntheticTokyoResponse] = []) {
        lock.lock()
        defer { lock.unlock() }
        planned = responses
        captured = []
    }

    func record(_ request: URLRequest) -> SyntheticTokyoResponse {
        let capturedRequest = CapturedTokyoRequest(
            url: request.url?.absoluteString ?? "",
            method: request.httpMethod ?? "",
            contentType: request.value(forHTTPHeaderField: "Content-Type"),
            idempotencyKey: request.value(forHTTPHeaderField: "Idempotency-Key"),
            authorization: request.value(forHTTPHeaderField: "Authorization"),
            body: Self.requestBody(request)
        )
        lock.lock()
        defer { lock.unlock() }
        captured.append(capturedRequest)
        return planned.isEmpty ? SyntheticTokyoResponse(status: 599, body: Data()) : planned.removeFirst()
    }

    func requests() -> [CapturedTokyoRequest] {
        lock.lock()
        defer { lock.unlock() }
        return captured
    }

    private static func requestBody(_ request: URLRequest) -> Data {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return Data() }
        stream.open()
        defer { stream.close() }
        var result = Data()
        var buffer = [UInt8](repeating: 0, count: 8192)
        while stream.hasBytesAvailable {
            let count = stream.read(&buffer, maxLength: buffer.count)
            if count <= 0 { break }
            result.append(buffer, count: count)
        }
        return result
    }
}

private final class SyntheticTokyoURLProtocol: URLProtocol, @unchecked Sendable {
    static let transport = SyntheticTokyoTransport()

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let plan = Self.transport.record(request)
        guard let url = request.url,
              let response = HTTPURLResponse(url: url, statusCode: plan.status, httpVersion: "HTTP/1.1", headerFields: ["Content-Type": "application/json"]) else {
            client?.urlProtocol(self, didFailWithError: TokyoGatewayError.unexpectedResponse)
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: plan.body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

final class TokyoEndpointTests: XCTestCase {
    func testSyntheticUploadUsesPrivateEndpointExactBytesAndStableRetryIdentity() async throws {
        let endpoint = try XCTUnwrap(URL(string: "https://tokyo.synthetic-tailnet.ts.net"))
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let ledger = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        let event = HealthEvent(
            eventID: "synthetic-event", revision: 0, operation: "upsert", kind: "quantity",
            type: "HKQuantityTypeIdentifierHeartRate", sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
            startUTC: Date(timeIntervalSince1970: 1_700_000_000),
            endUTC: Date(timeIntervalSince1970: 1_700_000_001),
            value: 72, unit: "count/min", metadata: [:]
        )
        _ = try await ledger.apply(events: [event])
        let prepared = try await ledger.prepareBatch(deviceID: "synthetic-device")
        let batch = try XCTUnwrap(prepared)
        let receipt = TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: batch.eventCount,
                                   receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                   contentHash: batch.contentHash, commitSequence: 1)
        let responseBody = try JSONEncoder().encode(receipt)
        let transport = SyntheticTokyoURLProtocol.transport
        transport.reset([
            SyntheticTokyoResponse(status: 503, body: Data("synthetic outage".utf8)),
            SyntheticTokyoResponse(status: 200, body: responseBody)
        ])
        defer { transport.reset() }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SyntheticTokyoURLProtocol.self]
        let session = URLSession(configuration: configuration, delegate: TokyoRedirectBlocker(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let gateway = TokyoCloudGateway(session: session, endpointProvider: { endpoint },
                                        tokenProvider: { "synthetic-token" }, erasureUploadGate: {})

        do {
            _ = try await gateway.upload(batch)
            XCTFail("A failed transport response must not become a receipt")
        } catch TokyoGatewayError.server(let status, _) {
            XCTAssertEqual(status, 503)
        }
        let saved = try await gateway.upload(batch)
        XCTAssertEqual(saved.batchID, batch.id)
        XCTAssertEqual(saved.contentHash, batch.contentHash)
        let requests = transport.requests()
        XCTAssertEqual(requests.count, 2)
        for request in requests {
            XCTAssertEqual(request.url, "https://tokyo.synthetic-tailnet.ts.net/v1/health/batches")
            XCTAssertEqual(request.method, "POST")
            XCTAssertEqual(request.contentType, "application/json")
            XCTAssertEqual(request.idempotencyKey, batch.id)
            XCTAssertEqual(request.authorization, "Bearer synthetic-token")
            XCTAssertEqual(request.body, batch.body)
        }

        transport.reset([SyntheticTokyoResponse(status: 200, body: responseBody)])
        let untrusted = TokyoCloudGateway(
            session: session,
            endpointProvider: { URL(string: "https://public.example") },
            tokenProvider: { "synthetic-token" },
            erasureUploadGate: {}
        )
        do {
            _ = try await untrusted.upload(batch)
            XCTFail("An untrusted origin must be rejected before URLSession")
        } catch TokyoGatewayError.invalidPrivateEndpoint {}
        XCTAssertTrue(transport.requests().isEmpty)

        let wrongReceipt = TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: batch.eventCount,
                                        receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                        contentHash: "wrong-hash", commitSequence: 1)
        transport.reset([SyntheticTokyoResponse(status: 200, body: try JSONEncoder().encode(wrongReceipt))])
        do {
            _ = try await gateway.upload(batch)
            XCTFail("A receipt for different bytes must not be accepted")
        } catch TokyoGatewayError.hashMismatch {}
        XCTAssertEqual(transport.requests().count, 1)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: folder)
    }

    func testPrivateHTTPSRootOriginsAreAccepted() throws {
        for value in [
            "https://tokyo.example-tailnet.ts.net",
            "https://tokyo.example-tailnet.ts.net/",
            "https://tokyo.example-tailnet.ts.net:443",
            "https://TOKYO.EXAMPLE-TAILNET.TS.NET:443/"
        ] {
            let url = try XCTUnwrap(URL(string: value))
            XCTAssertNoThrow(try TokyoPrivateEndpoint.validate(url), value)
            XCTAssertNotNil(TokyoPrivateEndpoint.validatedURL(from: "  \(value)\n"), value)
        }
    }

    func testPublicLookalikeAndNonHTTPSOriginsAreRejected() throws {
        for value in [
            "http://tokyo.example-tailnet.ts.net",
            "https://example.com",
            "https://tokyo.example-tailnet.ts.net.evil.example",
            "https://tokyo.example-tailnet-ts.net",
            "https://ts.net",
            "https://.ts.net",
            "https://tokyo..ts.net",
            "https://-tokyo.example-tailnet.ts.net",
            "https://tokyo-.example-tailnet.ts.net",
            "https://tokyo.example-tailnet.ts.net.",
            "https://127.0.0.1",
            "https://100.64.0.1",
            "https://[::1]",
            "https://\(String(repeating: "a", count: 64)).example-tailnet.ts.net"
        ] {
            let url = try XCTUnwrap(URL(string: value))
            XCTAssertThrowsError(try TokyoPrivateEndpoint.validate(url), value) { error in
                guard case TokyoGatewayError.invalidPrivateEndpoint = error else {
                    return XCTFail("Unexpected validation error for \(value): \(error)")
                }
            }
            XCTAssertNil(TokyoPrivateEndpoint.validatedURL(from: value), value)
        }
    }

    func testCredentialsPathQueryFragmentAndOtherPortsAreRejected() throws {
        for value in [
            "https://user@tokyo.example-tailnet.ts.net",
            "https://user:password@tokyo.example-tailnet.ts.net",
            "https://@tokyo.example-tailnet.ts.net",
            "https://tokyo.example-tailnet.ts.net?",
            "https://tokyo.example-tailnet.ts.net?token=synthetic",
            "https://tokyo.example-tailnet.ts.net#",
            "https://tokyo.example-tailnet.ts.net#fragment",
            "https://tokyo.example-tailnet.ts.net/v1",
            "https://tokyo.example-tailnet.ts.net//",
            "https://tokyo.example-tailnet.ts.net/%2F",
            "https://tokyo.example-tailnet.ts.net/..",
            "https://tokyo.example-tailnet.ts.net:80",
            "https://tokyo.example-tailnet.ts.net:8443",
            "https://tokyo.example-tailnet.ts.net:0"
        ] {
            let url = try XCTUnwrap(URL(string: value))
            XCTAssertThrowsError(try TokyoPrivateEndpoint.validate(url), value)
        }
    }

    func testErasurePendingStatesRequireCoherentTimestamps() throws {
        let pendingMetrics = TokyoEraseResponse(
            erasureID: "erase-one", status: "pending_metrics",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: nil,
            backupsExpiredAt: "2026-09-18T00:00:30Z",
            backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertEqual(try pendingMetrics.validate(expectedErasureID: "erase-one"), .pendingMetrics)

        let pendingBackup = TokyoEraseResponse(
            erasureID: "erase-one", status: "pending_backup_expiry",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: "2026-09-18T00:01:00Z",
            backupsExpiredAt: nil, backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertEqual(try pendingBackup.validate(expectedErasureID: "erase-one"), .pendingBackupExpiry)

        let contradictory = TokyoEraseResponse(
            erasureID: "erase-one", status: "pending_metrics",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: "2026-09-18T00:01:00Z",
            backupsExpiredAt: nil, backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertThrowsError(try contradictory.validate(expectedErasureID: "erase-one"))
    }

    func testErasureCompleteRequiresMetricsAndBackupExpiry() throws {
        let complete = TokyoEraseResponse(
            erasureID: "erase-complete", status: "complete",
            requestedAt: "2026-09-18T00:00:00.000Z", metricsDeletedAt: "2026-09-18T00:01:00.000Z",
            backupsExpiredAt: "2026-09-18T00:02:00.000Z", backupDeleteBy: "2026-10-18T00:00:00.000Z"
        )
        XCTAssertEqual(try complete.validate(expectedErasureID: "erase-complete"), .complete)

        let missingBackupExpiry = TokyoEraseResponse(
            erasureID: "erase-complete", status: "complete",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: "2026-09-18T00:01:00Z",
            backupsExpiredAt: nil, backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertThrowsError(try missingBackupExpiry.validate(expectedErasureID: "erase-complete"))
    }

    func testErasureResponseRejectsWrongIdentityUnknownStateAndMalformedTime() throws {
        let valid = TokyoEraseResponse(
            erasureID: "erase-valid", status: "pending_metrics",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: nil, backupsExpiredAt: nil,
            backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertThrowsError(try valid.validate(expectedErasureID: "different-erasure"))

        let unknown = TokyoEraseResponse(
            erasureID: "erase-valid", status: "done",
            requestedAt: "2026-09-18T00:00:00Z", metricsDeletedAt: nil, backupsExpiredAt: nil,
            backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertThrowsError(try unknown.validate(expectedErasureID: "erase-valid"))

        let malformed = TokyoEraseResponse(
            erasureID: "erase-valid", status: "pending_metrics",
            requestedAt: "not-a-time", metricsDeletedAt: nil, backupsExpiredAt: nil,
            backupDeleteBy: "2026-10-18T00:00:00Z"
        )
        XCTAssertThrowsError(try malformed.validate(expectedErasureID: "erase-valid"))
    }

    func testEveryRedirectIsRejectedWithoutStartingAnyNetworkTask() throws {
        let blocker = TokyoRedirectBlocker()
        let session = URLSession(configuration: .ephemeral, delegate: blocker, delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let original = try XCTUnwrap(URL(string: "https://tokyo.example-tailnet.ts.net/v1/health/batches"))
        let task = session.dataTask(with: original)
        // The task remains suspended: call the redirect delegate directly with
        // synthetic requests to cover privacy behavior without sending any data.
        XCTAssertEqual(task.state, .suspended)
        for status in [301, 302, 303, 307, 308] {
            for destination in [
                "https://tokyo.example-tailnet.ts.net/other-path",
                "https://other.example-tailnet.ts.net/v1/health/batches",
                "https://public.example/v1/health/batches",
                "http://tokyo.example-tailnet.ts.net/v1/health/batches"
            ] {
                let url = try XCTUnwrap(URL(string: destination))
                let response = try XCTUnwrap(HTTPURLResponse(url: original, statusCode: status, httpVersion: "HTTP/1.1", headerFields: ["Location": destination]))
                var redirected = URLRequest(url: url)
                redirected.httpMethod = "POST"
                redirected.httpBody = Data("synthetic-body".utf8)
                redirected.setValue("Bearer synthetic-token", forHTTPHeaderField: "Authorization")
                let completed = expectation(description: "Reject \(status) to \(destination)")
                completed.assertForOverFulfill = true
                blocker.urlSession(session, task: task, willPerformHTTPRedirection: response, newRequest: redirected) { allowed in
                    XCTAssertNil(allowed, "Redirect must not forward any request, body, or credential")
                    completed.fulfill()
                }
                wait(for: [completed], timeout: 1)
            }
        }
        XCTAssertEqual(task.state, .suspended)
    }

    func testCancelledUploadPassDoesNotStartAnyNetworkRequest() async throws {
        let endpoint = try XCTUnwrap(URL(string: "https://tokyo.synthetic-tailnet.ts.net"))
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let ledger = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        let pageAnchor = Data([0x51, 0x52, 0x53])
        let event = HealthEvent(
            eventID: "synthetic-cancelled-page", revision: 1, operation: "upsert", kind: "quantity",
            type: "HKQuantityTypeIdentifierHeartRate", sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
            startUTC: Date(timeIntervalSince1970: 1_700_000_000),
            endUTC: Date(timeIntervalSince1970: 1_700_000_001),
            value: 72, unit: "count/min", metadata: [:]
        )
        let saved = try await ledger.apply(
            events: [event], anchor: pageAnchor, typeIdentifier: "HKQuantityTypeIdentifierHeartRate"
        )
        let persistedAnchor = try await ledger.anchor(for: "HKQuantityTypeIdentifierHeartRate")
        XCTAssertEqual(saved, 1)
        XCTAssertEqual(persistedAnchor, pageAnchor)
        let transport = SyntheticTokyoURLProtocol.transport
        transport.reset([SyntheticTokyoResponse(status: 200, body: Data())])
        defer { transport.reset() }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SyntheticTokyoURLProtocol.self]
        let session = URLSession(configuration: configuration, delegate: TokyoRedirectBlocker(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let gateway = TokyoCloudGateway(session: session, endpointProvider: { endpoint }, tokenProvider: { "synthetic-token" })
        let health = await MainActor.run { HealthKitManager() }
        let engine = SyncEngine(health: health, database: ledger, gateway: gateway)
        let previousConsent = BoazConfiguration.uploadConsent
        BoazConfiguration.uploadConsent = true
        defer { BoazConfiguration.uploadConsent = previousConsent }

        let result = await Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return await engine.uploadPending()
        }.value
        XCTAssertTrue(result.cancelled)
        XCTAssertNil(result.failure)
        XCTAssertTrue(transport.requests().isEmpty)
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.records, 1)
        XCTAssertEqual(counts.pending, 1)
        try await ledger.closeForTesting()
        if testRun?.failureCount == 0 { try FileManager.default.removeItem(at: folder) }
    }

    func testUnreadableErasureRecoveryFailsClosedBeforeUpload() async throws {
        enum SyntheticRecoveryReadFailure: Error { case unavailable }
        let endpoint = try XCTUnwrap(URL(string: "https://tokyo.synthetic-tailnet.ts.net"))
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let ledger = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        let event = HealthEvent(
            eventID: "synthetic-erasure-gate", revision: 1, operation: "upsert", kind: "quantity",
            type: "HKQuantityTypeIdentifierHeartRate", sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
            startUTC: Date(timeIntervalSince1970: 1_700_000_000),
            endUTC: Date(timeIntervalSince1970: 1_700_000_001),
            value: 72, unit: "count/min", metadata: [:]
        )
        _ = try await ledger.apply(events: [event], anchor: nil, typeIdentifier: "HKQuantityTypeIdentifierHeartRate")
        let transport = SyntheticTokyoURLProtocol.transport
        transport.reset([SyntheticTokyoResponse(status: 200, body: Data())])
        defer { transport.reset() }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [SyntheticTokyoURLProtocol.self]
        let session = URLSession(configuration: configuration, delegate: TokyoRedirectBlocker(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let gateway = TokyoCloudGateway(session: session, endpointProvider: { endpoint }, tokenProvider: { "synthetic-token" })
        let health = await MainActor.run { HealthKitManager() }
        let engine = SyncEngine(health: health, database: ledger, gateway: gateway,
                                erasureCredential: { throw SyntheticRecoveryReadFailure.unavailable })
        let previousConsent = BoazConfiguration.uploadConsent
        BoazConfiguration.uploadConsent = true
        defer { BoazConfiguration.uploadConsent = previousConsent }

        XCTAssertThrowsError(try TokyoErasureStore.requireNoPendingForUpload(
            read: { throw SyntheticRecoveryReadFailure.unavailable }
        ))
        XCTAssertThrowsError(try TokyoErasureStore.requireNoPendingForUpload(
            read: { TokyoErasureCredential(id: "synthetic-erasure", secret: String(repeating: "a", count: 64)) }
        ))
        let result = await engine.uploadPending()
        XCTAssertNotNil(result.failure)
        XCTAssertEqual(result.committedEvents, 0)
        XCTAssertTrue(transport.requests().isEmpty, "No request may escape when recovery state is unreadable")
        let counts = try await ledger.counts()
        XCTAssertEqual(counts.records, 1)
        XCTAssertEqual(counts.pending, 1)
        try await ledger.closeForTesting()
        if testRun?.failureCount == 0 { try FileManager.default.removeItem(at: folder) }
    }
}
