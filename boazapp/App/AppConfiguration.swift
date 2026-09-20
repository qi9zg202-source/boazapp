import Foundation
import Security

enum BoazConfiguration {
    static let consentKey = "boaz.health.cloudUploadConsent"
    static let endpointKey = "boaz.health.tokyoEndpoint"
    static let deviceIDKey = "boaz.health.deviceID"
    static let deviceIdentityTransitionsKey = "boaz.health.deviceIdentityTransitions"

    private struct DeviceIdentityTransitions: Codable {
        var latestErasureID: String?
        var replacements: [String: String]
    }

    static var deviceID: String {
        deviceID(in: .standard)
    }

    static func deviceID(in defaults: UserDefaults) -> String {
        if let id = defaults.string(forKey: deviceIDKey), !id.isEmpty { return id }
        let id = UUID().uuidString.lowercased()
        defaults.set(id, forKey: deviceIDKey)
        return id
    }

    /// A completed cloud erasure creates a new upload identity. Persisting the
    /// replacement before installing it makes a retry after any interruption
    /// reuse the same identity instead of generating another one.
    @discardableResult
    static func rotateDeviceID(afterCompletedErasure erasureID: String,
                               in defaults: UserDefaults = .standard) throws -> String {
        guard !erasureID.isEmpty else { throw TokyoGatewayError.unexpectedResponse }
        let decoder = JSONDecoder()
        var transitions: DeviceIdentityTransitions
        if let data = defaults.data(forKey: deviceIdentityTransitionsKey) {
            transitions = try decoder.decode(DeviceIdentityTransitions.self, from: data)
        } else {
            transitions = DeviceIdentityTransitions(latestErasureID: nil, replacements: [:])
        }

        if let replacement = transitions.replacements[erasureID] {
            // Only the latest completion may repair an interrupted install.
            // Replayed older receipts must never roll identity backwards.
            if transitions.latestErasureID == erasureID {
                defaults.set(replacement, forKey: deviceIDKey)
            }
            return deviceID(in: defaults)
        }

        let replacement = UUID().uuidString.lowercased()
        transitions.latestErasureID = erasureID
        transitions.replacements[erasureID] = replacement
        defaults.set(try JSONEncoder().encode(transitions), forKey: deviceIdentityTransitionsKey)
        defaults.set(replacement, forKey: deviceIDKey)
        return replacement
    }

    static var uploadConsent: Bool {
        get { UserDefaults.standard.bool(forKey: consentKey) }
        set { UserDefaults.standard.set(newValue, forKey: consentKey) }
    }

    static var endpoint: URL? {
        guard let value = UserDefaults.standard.string(forKey: endpointKey) else { return nil }
        return URL(string: value)
    }

    static func setEndpoint(_ url: URL?) {
        UserDefaults.standard.set(url?.absoluteString, forKey: endpointKey)
    }
}

enum CredentialError: Error, LocalizedError {
    case unavailable(OSStatus)

    var errorDescription: String? {
        switch self {
        case .unavailable: "The device credential is unavailable in Keychain."
        }
    }
}

enum TokyoCredentialStore {
    private static let service = "com.boaz.health.tokyo"
    private static let account = "device-token"

    static func token() throws -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        var value: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &value)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess, let data = value as? Data, let token = String(data: data, encoding: .utf8) else {
            throw CredentialError.unavailable(status)
        }
        return token
    }

    static func save(_ token: String) throws {
        try delete()
        let attributes: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            kSecValueData as String: Data(token.utf8)
        ]
        let status = SecItemAdd(attributes as CFDictionary, nil)
        guard status == errSecSuccess else { throw CredentialError.unavailable(status) }
    }

    static func delete() throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account
        ]
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw CredentialError.unavailable(status) }
    }
}

struct TokyoErasureCredential: Codable, Sendable {
    let id: String
    let secret: String
}

enum TokyoErasureStore {
    private static let service = "com.boaz.health.tokyo"
    private static let account = "erasure-recovery"

    static func current() throws -> TokyoErasureCredential? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        var value: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &value)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess, let data = value as? Data else { throw CredentialError.unavailable(status) }
        return try JSONDecoder().decode(TokyoErasureCredential.self, from: data)
    }

    /// A missing recovery record is the only state that permits upload. A
    /// Keychain or decoding failure must propagate and keep the upload gate
    /// closed instead of being interpreted as absence.
    static func requireNoPendingForUpload(
        read: () throws -> TokyoErasureCredential? = TokyoErasureStore.current
    ) throws {
        guard try read() == nil else { throw TokyoGatewayError.erasurePending }
    }

    static func createIfNeeded() throws -> TokyoErasureCredential {
        if let existing = try current() { return existing }
        var bytes = [UInt8](repeating: 0, count: 32)
        let randomStatus = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        guard randomStatus == errSecSuccess else { throw CredentialError.unavailable(randomStatus) }
        let credential = TokyoErasureCredential(id: UUID().uuidString.lowercased(), secret: bytes.map { String(format: "%02x", $0) }.joined())
        let data = try JSONEncoder().encode(credential)
        let attributes: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
            kSecValueData as String: data
        ]
        let status = SecItemAdd(attributes as CFDictionary, nil)
        guard status == errSecSuccess else { throw CredentialError.unavailable(status) }
        return credential
    }

    static func delete() throws {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account
        ]
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else { throw CredentialError.unavailable(status) }
    }
}
