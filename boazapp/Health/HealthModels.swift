import Foundation

/// An immutable HealthKit fact or a day-level activity projection. Dates are UTC instants
/// when encoded by the network layer; HealthKit deletion tombstones have no dates/source.
struct HealthEvent: Codable, Sendable, Equatable {
    let eventID: String
    let revision: Int
    let operation: String // "upsert" or "delete"
    let kind: String // "quantity", "category", "workout", or "activity"
    let type: String
    let sourceBundleID: String
    let sourceName: String
    let startUTC: Date?
    let endUTC: Date?
    let value: Double?
    let unit: String?
    let metadata: [String: String]
}

/// The caller must commit all events and `nextAnchorData` in one SQLite transaction.
/// `countReachedLimit` means another query is required; it does not prove completeness.
struct HealthImportPage: Sendable {
    let typeIdentifier: String
    let events: [HealthEvent]
    let nextAnchorData: Data
    let countReachedLimit: Bool
}

struct TodayCumulative: Sendable {
    let dayStart: Date
    let asOf: Date
    let steps: Double?
    let flights: Double?
    let activeEnergyKcal: Double?
    let exerciseMinutes: Double?
    let failedTypes: [String]
}

struct WorkoutEventPage: Sendable {
    let events: [HealthEvent]
    let nextOffset: Int?
}

struct BackgroundDeliveryReport: Sendable {
    let enabledTypes: [String]
    let failures: [String: String]
}

enum HealthCollectionError: LocalizedError {
    case unavailable
    case unsupportedType(String)
    case invalidAnchor
    case noAnchorReturned
    case unconvertibleSample(String)
    case authorizationRequestFailed
    case invalidDateRange

    var errorDescription: String? {
        switch self {
        case .unavailable: "Health data is unavailable on this device."
        case .unsupportedType(let type): "Unsupported HealthKit type: \(type)"
        case .invalidAnchor: "The stored HealthKit anchor could not be decoded."
        case .noAnchorReturned: "HealthKit did not return a new anchor."
        case .unconvertibleSample(let id): "HealthKit sample \(id) could not be converted."
        case .authorizationRequestFailed: "HealthKit did not complete the read request."
        case .invalidDateRange: "The activity date range is invalid."
        }
    }
}
