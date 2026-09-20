import Foundation

enum TokyoGatewayError: Error, LocalizedError {
    case invalidPrivateEndpoint
    case invalidPairingCode
    case notPaired
    case erasurePending
    case server(Int, String)
    case unexpectedResponse
    case hashMismatch

    var errorDescription: String? {
        switch self {
        case .invalidPrivateEndpoint: "Enter a private HTTPS address ending in .ts.net."
        case .invalidPairingCode: "Enter the 64-character one-time pairing code."
        case .notPaired: "Pair this iPhone with your Tokyo service first."
        case .erasurePending: "Cloud erasure is pending; upload remains blocked."
        case .server(let code, let detail): "Tokyo returned \(code): \(detail)"
        case .unexpectedResponse: "Tokyo returned an unreadable response."
        case .hashMismatch: "Tokyo's receipt did not match this upload."
        }
    }
}

enum TokyoCredentialFormat {
    static func isHex64(_ value: String) -> Bool {
        value.utf8.count == 64 && value.utf8.allSatisfy {
            (48...57).contains($0) || (65...70).contains($0) || (97...102).contains($0)
        }
    }

    static func normalizedHex64(_ value: String) -> String? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return isHex64(trimmed) ? trimmed.lowercased() : nil
    }
}

enum TokyoPrivateEndpoint {
    /// Pairing accepts an origin, so appended API paths cannot inherit a path,
    /// credentials, or query supplied by an untrusted configuration string.
    static func validate(_ url: URL) throws {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              components.scheme?.lowercased() == "https",
              let host = components.host?.lowercased(), host.hasSuffix(".ts.net"),
              host.utf8.count <= 253,
              host.split(separator: ".", omittingEmptySubsequences: false).allSatisfy({ label in
                  !label.isEmpty && label.utf8.count <= 63 && label.first != "-" && label.last != "-"
                      && label.utf8.allSatisfy { (97...122).contains($0) || (48...57).contains($0) || $0 == 45 }
              }),
              components.user == nil, components.password == nil,
              components.query == nil, components.fragment == nil,
              components.percentEncodedPath.isEmpty || components.percentEncodedPath == "/",
              components.port == nil || components.port == 443 else {
            throw TokyoGatewayError.invalidPrivateEndpoint
        }
    }

    static func validatedURL(from text: String) -> URL? {
        guard let url = URL(string: text.trimmingCharacters(in: .whitespacesAndNewlines)) else { return nil }
        do {
            try validate(url)
            return url
        } catch {
            return nil
        }
    }
}

private enum TokyoTimestamp {
    static func parse(_ value: String) -> Date? {
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return fractional.date(from: value) ?? ISO8601DateFormatter().date(from: value)
    }
}

/// Reject every redirect, including same-origin redirects. The original 3xx
/// response reaches the gateway's error handling and the durable batch stays queued.
final class TokyoRedirectBlocker: NSObject, URLSessionTaskDelegate {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping @Sendable (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

struct TokyoReceipt: Codable, Sendable {
    let batchID: String
    let status: String
    let acceptedEvents: Int
    let receivedAt: String
    let projectedAt: String?
    let contentHash: String?
    let commitSequence: Int?

    enum CodingKeys: String, CodingKey {
        case batchID = "batch_id", status, acceptedEvents = "accepted_events", receivedAt = "received_at"
        case projectedAt = "projected_at", contentHash = "content_hash", commitSequence = "commit_sequence"
    }

    /// Every state change must be tied to the exact durable request, including polling.
    func validate(for batch: PreparedHealthBatch) throws {
        guard batchID == batch.id, acceptedEvents == batch.eventCount,
              let commitSequence, commitSequence > 0,
              status == "cloud_saved" || status == "metrics_current",
              TokyoTimestamp.parse(receivedAt) != nil else { throw TokyoGatewayError.unexpectedResponse }
        guard contentHash == batch.contentHash else { throw TokyoGatewayError.hashMismatch }
        if status == "metrics_current" {
            guard let projectedAt, TokyoTimestamp.parse(projectedAt) != nil else {
                throw TokyoGatewayError.unexpectedResponse
            }
        } else if projectedAt != nil {
            throw TokyoGatewayError.unexpectedResponse
        }
    }
}

struct TokyoPairingResponse: Codable, Sendable {
    let deviceID: String
    let token: String

    enum CodingKeys: String, CodingKey { case deviceID = "device_id", token }
}

struct TokyoPairingRequest: Encodable, Sendable {
    let code: String
    let deviceID: String

    enum CodingKeys: String, CodingKey { case code, deviceID = "device_id" }
}

struct TokyoEraseRequest: Encodable, Sendable {
    let confirmation: String
    let erasureID: String
    let erasureSecret: String

    enum CodingKeys: String, CodingKey {
        case confirmation, erasureID = "erasure_id", erasureSecret = "erasure_secret"
    }
}

struct TokyoEraseResponse: Decodable, Sendable {
    let erasureID: String
    let status: String
    let requestedAt: String
    let metricsDeletedAt: String?
    let backupsExpiredAt: String?
    let backupDeleteBy: String?

    enum CodingKeys: String, CodingKey {
        case erasureID = "erasure_id", status, requestedAt = "requested_at"
        case metricsDeletedAt = "metrics_deleted_at", backupsExpiredAt = "backups_expired_at", backupDeleteBy = "backup_delete_by"
    }

    func validate(expectedErasureID: String) throws -> TokyoErasureStatus {
        guard erasureID == expectedErasureID,
              let state = TokyoErasureStatus(rawValue: status),
              let requested = TokyoTimestamp.parse(requestedAt),
              let backupDeleteBy,
              let backupDeadline = TokyoTimestamp.parse(backupDeleteBy),
              backupDeadline >= requested else {
            throw TokyoGatewayError.unexpectedResponse
        }

        let metricsDeleted = try validatedOptionalTimestamp(metricsDeletedAt, notBefore: requested)
        let backupsExpired = try validatedOptionalTimestamp(backupsExpiredAt, notBefore: requested)
        switch state {
        case .pendingMetrics:
            // Backup pruning is an independent worker and may finish before
            // metrics deletion. The server still names that state
            // `pending_metrics`, so only the metrics timestamp must be absent.
            guard metricsDeleted == nil else {
                throw TokyoGatewayError.unexpectedResponse
            }
        case .pendingBackupExpiry:
            guard metricsDeleted != nil, backupsExpired == nil else {
                throw TokyoGatewayError.unexpectedResponse
            }
        case .complete:
            guard metricsDeleted != nil, backupsExpired != nil else {
                throw TokyoGatewayError.unexpectedResponse
            }
        }
        return state
    }

    private func validatedOptionalTimestamp(_ value: String?, notBefore lowerBound: Date) throws -> Date? {
        guard let value else { return nil }
        guard let date = TokyoTimestamp.parse(value), date >= lowerBound else {
            throw TokyoGatewayError.unexpectedResponse
        }
        return date
    }
}

enum TokyoErasureStatus: String, Sendable {
    case pendingMetrics = "pending_metrics"
    case pendingBackupExpiry = "pending_backup_expiry"
    case complete
}

actor TokyoCloudGateway {
    private let session: URLSession
    private let endpointProvider: @Sendable () -> URL?
    private let tokenProvider: @Sendable () throws -> String?
    private let erasureUploadGate: @Sendable () throws -> Void

    /// The production request path always checks the Keychain recovery record
    /// before it sends a request carrying the upload credential.
    init(
        session: URLSession? = nil,
        endpointProvider: @escaping @Sendable () -> URL? = { BoazConfiguration.endpoint },
        tokenProvider: @escaping @Sendable () throws -> String? = { try TokyoCredentialStore.token() }
    ) {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 5
        configuration.timeoutIntervalForResource = 10
        configuration.waitsForConnectivity = false
        self.session = session ?? URLSession(configuration: configuration, delegate: TokyoRedirectBlocker(), delegateQueue: nil)
        self.endpointProvider = endpointProvider
        self.tokenProvider = tokenProvider
        self.erasureUploadGate = { try TokyoErasureStore.requireNoPendingForUpload() }
    }

    #if DEBUG
    /// Only a Debug build can substitute the erasure gate for synthetic
    /// transport tests. Release builds expose no initializer that bypasses it.
    init(
        session: URLSession,
        endpointProvider: @escaping @Sendable () -> URL?,
        tokenProvider: @escaping @Sendable () throws -> String?,
        erasureUploadGate: @escaping @Sendable () throws -> Void
    ) {
        self.session = session
        self.endpointProvider = endpointProvider
        self.tokenProvider = tokenProvider
        self.erasureUploadGate = erasureUploadGate
    }
    #endif

    func pair(endpoint: URL, code: String, deviceID: String) async throws {
        try TokyoPrivateEndpoint.validate(endpoint)
        guard let code = TokyoCredentialFormat.normalizedHex64(code) else {
            throw TokyoGatewayError.invalidPairingCode
        }
        let body = try JSONEncoder().encode(TokyoPairingRequest(code: code, deviceID: deviceID))
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/pairings"))
        request.httpMethod = "POST"
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let data = try await send(request)
        let pairing = try JSONDecoder().decode(TokyoPairingResponse.self, from: data)
        guard pairing.deviceID == deviceID, TokyoCredentialFormat.isHex64(pairing.token) else {
            throw TokyoGatewayError.unexpectedResponse
        }
        try TokyoCredentialStore.save(pairing.token)
        BoazConfiguration.setEndpoint(endpoint)
    }

    func upload(_ batch: PreparedHealthBatch) async throws -> TokyoReceipt {
        try erasureUploadGate()
        let endpoint = try configuredEndpoint()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/batches"))
        request.httpMethod = "POST"
        request.httpBody = batch.body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(batch.id, forHTTPHeaderField: "Idempotency-Key")
        try authorize(&request)
        let data = try await send(request)
        let receipt = try JSONDecoder().decode(TokyoReceipt.self, from: data)
        try receipt.validate(for: batch)
        return receipt
    }

    func receipt(batchID: String) async throws -> TokyoReceipt? {
        try erasureUploadGate()
        let endpoint = try configuredEndpoint()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/batches/\(batchID)/receipt"))
        request.httpMethod = "GET"
        try authorize(&request)
        try Task.checkCancellation()
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw TokyoGatewayError.unexpectedResponse }
        if http.statusCode == 404 { return nil }
        guard (200..<300).contains(http.statusCode) else { throw TokyoGatewayError.server(http.statusCode, String(decoding: data.prefix(200), as: UTF8.self)) }
        let receipt = try JSONDecoder().decode(TokyoReceipt.self, from: data)
        guard receipt.batchID == batchID else { throw TokyoGatewayError.unexpectedResponse }
        return receipt
    }

    func eraseCloudCopy() async throws -> TokyoEraseResponse {
        let endpoint = try configuredEndpoint()
        let prior = try TokyoErasureStore.current()
        if prior == nil, try TokyoCredentialStore.token() == nil { throw TokyoGatewayError.notPaired }
        let recovery = try TokyoErasureStore.createIfNeeded()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/erase"))
        request.httpMethod = "POST"
        request.httpBody = try JSONEncoder().encode(TokyoEraseRequest(
            confirmation: "ERASE_CLOUD_HEALTH_DATA", erasureID: recovery.id, erasureSecret: recovery.secret
        ))
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if let token = try TokyoCredentialStore.token() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        } else {
            request.setValue("Bearer \(recovery.secret)", forHTTPHeaderField: "Authorization")
        }
        let data: Data
        do {
            data = try await send(request)
        } catch TokyoGatewayError.server(401, _) where prior != nil {
            request.setValue("Bearer \(recovery.secret)", forHTTPHeaderField: "Authorization")
            data = try await send(request)
        }
        let response = try JSONDecoder().decode(TokyoEraseResponse.self, from: data)
        _ = try response.validate(expectedErasureID: recovery.id)
        return response
    }

    func erasureStatus() async throws -> TokyoEraseResponse? {
        guard let recovery = try TokyoErasureStore.current() else { return nil }
        let endpoint = try configuredEndpoint()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/erasures/\(recovery.id)"))
        request.httpMethod = "GET"
        request.setValue("Bearer \(recovery.secret)", forHTTPHeaderField: "Authorization")
        let data = try await send(request)
        let response = try JSONDecoder().decode(TokyoEraseResponse.self, from: data)
        _ = try response.validate(expectedErasureID: recovery.id)
        return response
    }

    private func configuredEndpoint() throws -> URL {
        guard let endpoint = endpointProvider() else { throw TokyoGatewayError.notPaired }
        try TokyoPrivateEndpoint.validate(endpoint)
        return endpoint
    }

    private func authorize(_ request: inout URLRequest) throws {
        guard let token = try tokenProvider(), !token.isEmpty else { throw TokyoGatewayError.notPaired }
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }

    private func send(_ request: URLRequest) async throws -> Data {
        try Task.checkCancellation()
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw TokyoGatewayError.unexpectedResponse }
        guard (200..<300).contains(http.statusCode) else {
            throw TokyoGatewayError.server(http.statusCode, String(decoding: data.prefix(200), as: UTF8.self))
        }
        return data
    }

}
