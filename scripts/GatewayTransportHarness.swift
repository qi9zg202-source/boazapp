import Foundation
@testable import boazapp

/// Runs the production gateway's upload method against an in-process URLProtocol.
/// The private-looking hostname is never resolved and all records/credentials are synthetic.
private struct FixtureResponse: Sendable {
    let status: Int
    let body: Data
}

private struct CapturedRequest: Sendable {
    let url: String
    let method: String
    let contentType: String?
    let idempotencyKey: String?
    let authorization: String?
    let body: Data
}

private final class FixtureState: @unchecked Sendable {
    private let lock = NSLock()
    private var responses: [FixtureResponse] = []
    private var captured: [CapturedRequest] = []

    func reset(_ values: [FixtureResponse] = []) {
        lock.lock()
        defer { lock.unlock() }
        responses = values
        captured = []
    }

    func record(_ request: URLRequest) -> FixtureResponse {
        let entry = CapturedRequest(
            url: request.url?.absoluteString ?? "",
            method: request.httpMethod ?? "",
            contentType: request.value(forHTTPHeaderField: "Content-Type"),
            idempotencyKey: request.value(forHTTPHeaderField: "Idempotency-Key"),
            authorization: request.value(forHTTPHeaderField: "Authorization"),
            body: Self.requestBody(request)
        )
        lock.lock()
        defer { lock.unlock() }
        captured.append(entry)
        return responses.isEmpty ? FixtureResponse(status: 599, body: Data()) : responses.removeFirst()
    }

    func requests() -> [CapturedRequest] {
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

private final class FixtureURLProtocol: URLProtocol, @unchecked Sendable {
    static let state = FixtureState()

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let plan = Self.state.record(request)
        guard let url = request.url,
              let response = HTTPURLResponse(url: url, statusCode: plan.status, httpVersion: "HTTP/1.1", headerFields: ["Content-Type": "application/json"]) else {
            client?.urlProtocol(self, didFailWithError: GatewayTransportHarness.Failure.assertion("Cannot create fixture response"))
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: plan.body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

struct GatewayTransportHarness {
    enum Failure: Error { case assertion(String) }

    static func require(_ condition: Bool, _ message: String) throws {
        if !condition { throw Failure.assertion(message) }
    }

    static func run(schema: URL, directory: URL) async throws {
        let endpoint = URL(string: "https://tokyo.synthetic-tailnet.ts.net")!
        let ledger = try BoazLocalDatabase(url: directory.appendingPathComponent("gateway.sqlite"), schemaURL: schema)
        let event = HealthEvent(
            eventID: "synthetic-event", revision: 0, operation: "upsert", kind: "quantity",
            type: "HKQuantityTypeIdentifierHeartRate", sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
            startUTC: Date(timeIntervalSince1970: 1_700_000_000),
            endUTC: Date(timeIntervalSince1970: 1_700_000_001),
            value: 72, unit: "count/min", metadata: [:]
        )
        _ = try await ledger.apply(events: [event])
        guard let batch = try await ledger.prepareBatch(deviceID: "synthetic-device") else {
            throw Failure.assertion("No SQLite outbox batch")
        }
        let receipt = TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: batch.eventCount,
                                   receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                                   contentHash: batch.contentHash, commitSequence: 1)
        let state = FixtureURLProtocol.state
        state.reset([
            FixtureResponse(status: 503, body: Data("synthetic outage".utf8)),
            FixtureResponse(status: 200, body: try JSONEncoder().encode(receipt))
        ])
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [FixtureURLProtocol.self]
        let session = URLSession(configuration: configuration, delegate: TokyoRedirectBlocker(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }
        let gateway = TokyoCloudGateway(session: session, endpointProvider: { endpoint },
                                        tokenProvider: { "synthetic-token" }, erasureUploadGate: {})

        do {
            _ = try await gateway.upload(batch)
            throw Failure.assertion("HTTP 503 was accepted")
        } catch TokyoGatewayError.server(let status, _) {
            try require(status == 503, "Expected HTTP 503")
        }
        let saved = try await gateway.upload(batch)
        try require(saved.batchID == batch.id && saved.contentHash == batch.contentHash, "Matching receipt was rejected")
        let requests = state.requests()
        try require(requests.count == 2, "Expected two isolated upload attempts")
        for request in requests {
            try require(request.url == "https://tokyo.synthetic-tailnet.ts.net/v1/health/batches", "Wrong upload URL")
            try require(request.method == "POST", "Wrong method")
            try require(request.contentType == "application/json", "Wrong content type")
            try require(request.idempotencyKey == batch.id, "Retry changed idempotency key")
            try require(request.authorization == "Bearer synthetic-token", "Missing synthetic authorization")
            try require(request.body == batch.body, "Retry changed request bytes")
        }
        print("PASS GATEWAY-01: actual upload path keeps private URL, token, batch ID and exact bytes across HTTP retry")

        state.reset([FixtureResponse(status: 200, body: try JSONEncoder().encode(receipt))])
        let publicGateway = TokyoCloudGateway(
            session: session, endpointProvider: { URL(string: "https://public.example") },
            tokenProvider: { "synthetic-token" }, erasureUploadGate: {}
        )
        do {
            _ = try await publicGateway.upload(batch)
            throw Failure.assertion("Public endpoint was accepted")
        } catch TokyoGatewayError.invalidPrivateEndpoint {}
        try require(state.requests().isEmpty, "Public endpoint reached transport")
        print("PASS GATEWAY-02: public endpoint rejected before transport")

        let bad = TokyoReceipt(batchID: batch.id, status: "cloud_saved", acceptedEvents: batch.eventCount,
                               receivedAt: "2026-09-18T00:00:00Z", projectedAt: nil,
                               contentHash: "wrong-hash", commitSequence: 1)
        state.reset([FixtureResponse(status: 200, body: try JSONEncoder().encode(bad))])
        do {
            _ = try await gateway.upload(batch)
            throw Failure.assertion("Unbound receipt was accepted")
        } catch TokyoGatewayError.hashMismatch {}
        try require(state.requests().count == 1, "Expected one mismatched receipt attempt")
        print("PASS GATEWAY-03: mismatched Rust receipt rejected")
        try await ledger.closeForTesting()
        print("RESULT 3/3 synthetic gateway scenarios passed; no remote calls")
    }
}
