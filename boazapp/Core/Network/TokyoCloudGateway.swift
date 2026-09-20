import Foundation

enum TokyoGatewayError: Error, LocalizedError {
    case invalidPrivateEndpoint
    case notPaired
    case server(Int, String)
    case unexpectedResponse
    case hashMismatch

    var errorDescription: String? {
        switch self {
        case .invalidPrivateEndpoint: "Enter a private HTTPS address ending in .ts.net."
        case .notPaired: "Pair this iPhone with your Tokyo service first."
        case .server(let code, let detail): "Tokyo returned \(code): \(detail)"
        case .unexpectedResponse: "Tokyo returned an unreadable response."
        case .hashMismatch: "Tokyo's receipt did not match this upload."
        }
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
}

struct TokyoPairingResponse: Codable, Sendable {
    let deviceID: String
    let token: String

    enum CodingKeys: String, CodingKey { case deviceID = "device_id", token }
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
}

actor TokyoCloudGateway {
    private let session: URLSession

    init() {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 5
        configuration.timeoutIntervalForResource = 10
        configuration.waitsForConnectivity = false
        session = URLSession(configuration: configuration)
    }

    func pair(endpoint: URL, code: String, deviceID: String) async throws {
        try Self.validate(endpoint)
        let body = try JSONSerialization.data(withJSONObject: ["code": code, "device_id": deviceID], options: [.sortedKeys])
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/pairings"))
        request.httpMethod = "POST"
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let data = try await send(request)
        let pairing = try JSONDecoder().decode(TokyoPairingResponse.self, from: data)
        guard pairing.deviceID == deviceID, !pairing.token.isEmpty else { throw TokyoGatewayError.unexpectedResponse }
        try TokyoCredentialStore.save(pairing.token)
        BoazConfiguration.setEndpoint(endpoint)
    }

    func upload(_ batch: PreparedHealthBatch) async throws -> TokyoReceipt {
        let endpoint = try configuredEndpoint()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/batches"))
        request.httpMethod = "POST"
        request.httpBody = batch.body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(batch.id, forHTTPHeaderField: "Idempotency-Key")
        try authorize(&request)
        let data = try await send(request)
        let receipt = try JSONDecoder().decode(TokyoReceipt.self, from: data)
        guard receipt.batchID == batch.id, receipt.acceptedEvents == batch.eventCount else { throw TokyoGatewayError.unexpectedResponse }
        if let hash = receipt.contentHash, hash != batch.contentHash { throw TokyoGatewayError.hashMismatch }
        return receipt
    }

    func receipt(batchID: String) async throws -> TokyoReceipt? {
        let endpoint = try configuredEndpoint()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/batches/\(batchID)/receipt"))
        request.httpMethod = "GET"
        try authorize(&request)
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw TokyoGatewayError.unexpectedResponse }
        if http.statusCode == 404 { return nil }
        guard (200..<300).contains(http.statusCode) else { throw TokyoGatewayError.server(http.statusCode, String(decoding: data.prefix(200), as: UTF8.self)) }
        return try JSONDecoder().decode(TokyoReceipt.self, from: data)
    }

    func eraseCloudCopy() async throws -> TokyoEraseResponse {
        let endpoint = try configuredEndpoint()
        let prior = try TokyoErasureStore.current()
        if prior == nil, try TokyoCredentialStore.token() == nil { throw TokyoGatewayError.notPaired }
        let recovery = try TokyoErasureStore.createIfNeeded()
        var request = URLRequest(url: endpoint.appendingPathComponent("v1/health/erase"))
        request.httpMethod = "POST"
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "confirmation": "ERASE_CLOUD_HEALTH_DATA", "erasure_id": recovery.id, "erasure_secret": recovery.secret
        ], options: [.sortedKeys])
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
        guard response.erasureID == recovery.id else { throw TokyoGatewayError.unexpectedResponse }
        try TokyoCredentialStore.delete()
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
        guard response.erasureID == recovery.id else { throw TokyoGatewayError.unexpectedResponse }
        try TokyoCredentialStore.delete()
        return response
    }

    private func configuredEndpoint() throws -> URL {
        guard let endpoint = BoazConfiguration.endpoint else { throw TokyoGatewayError.notPaired }
        try Self.validate(endpoint)
        return endpoint
    }

    private func authorize(_ request: inout URLRequest) throws {
        guard let token = try TokyoCredentialStore.token() else { throw TokyoGatewayError.notPaired }
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }

    private func send(_ request: URLRequest) async throws -> Data {
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else { throw TokyoGatewayError.unexpectedResponse }
        guard (200..<300).contains(http.statusCode) else {
            throw TokyoGatewayError.server(http.statusCode, String(decoding: data.prefix(200), as: UTF8.self))
        }
        return data
    }

    private static func validate(_ url: URL) throws {
        guard url.scheme == "https", let host = url.host?.lowercased(), host.hasSuffix(".ts.net"),
              url.user == nil, url.password == nil, url.query == nil, url.fragment == nil else {
            throw TokyoGatewayError.invalidPrivateEndpoint
        }
    }
}
