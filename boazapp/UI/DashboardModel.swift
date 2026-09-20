import Foundation
import Combine

/// Presentation data only. The HealthKit and SQLite layers remain authoritative.
struct MetricDisplay: Sendable {
    var value: Double?
    var unit: String
    var observedAt: Date?
    var sampleCount: Int
    var precision: Int
    var detail: String?

    init(value: Double?, unit: String, observedAt: Date? = nil, sampleCount: Int = 0, precision: Int = 0, detail: String? = nil) {
        self.value = value
        self.unit = unit
        self.observedAt = observedAt
        self.sampleCount = sampleCount
        self.precision = precision
        self.detail = detail
    }

    var formattedValue: String {
        guard let value, value.isFinite else { return "—" }
        return value.formatted(.number.precision(.fractionLength(precision)))
    }
}

struct SleepDisplay: Sendable {
    var startedAt: Date? = nil
    var endedAt: Date? = nil
    var totalMinutes: Double
    var deepMinutes: Double = 0
    var coreMinutes: Double = 0
    var remMinutes: Double = 0
    var awakeMinutes: Double = 0
    var unspecifiedMinutes: Double = 0
    var inBedMinutes: Double? = nil
    var sourceCount: Int = 0
    var wristTemperatureC: Double? = nil
    var overnightHeartRate: Double? = nil
    var overnightRespiratoryRate: Double? = nil
    var overnightOxygenPercent: Double? = nil
    var overnightHRVMilliseconds: Double? = nil

    var efficiencyPercent: Double? {
        guard let inBedMinutes, inBedMinutes > 0, totalMinutes >= 0,
              totalMinutes <= inBedMinutes else { return nil }
        return totalMinutes / inBedMinutes * 100
    }
}

struct FitnessDisplay: Sendable {
    var day: Date? = nil
    var moveKcal: Double? = nil
    var moveGoalKcal: Double? = nil
    var exerciseMinutes: Double? = nil
    var exerciseGoalMinutes: Double? = nil
    var standHours: Double? = nil
    var standGoalHours: Double? = nil
    var steps: Double? = nil
    var flights: Double? = nil
    var recentWorkouts: [WorkoutDisplay] = []
}

struct WorkoutDisplay: Identifiable, Sendable {
    var id: String
    var title: String
    var startedAt: Date
    var durationMinutes: Double
    var energyKcal: Double? = nil
    var distance: MetricDisplay? = nil
    var heartRate: MetricDisplay? = nil
    var eventCount: Int = 0
}

struct VitalsDisplay: Sendable {
    var heartRate: Double? = nil
    var restingHeartRate: Double? = nil
    var oxygenPercent: Double? = nil
    var weightKg: Double? = nil
    var hrvMilliseconds: Double? = nil
    var systolicMMHg: Double? = nil
    var diastolicMMHg: Double? = nil
    var bodyFatPercent: Double? = nil
    var bmi: Double? = nil
    var timestamps: [String: Date] = [:]
    var sampleCounts: [String: Int] = [:]
}

enum AuditSeverity: Sendable {
    case information, success, warning, failure
}

struct SyncAuditEntry: Identifiable, Sendable {
    var id: String
    var occurredAt: Date
    var title: String
    var detail: String
    var severity: AuditSeverity
}

struct DashboardSnapshot: Sendable {
    var sleep: SleepDisplay?
    var fitness: FitnessDisplay?
    var vitals: VitalsDisplay? = nil
    var localSampleCount: Int = 0
    var pendingSampleCount: Int = 0
    var lastImportedAt: Date?
    var lastCloudReceiptAt: Date?
    var lastProjectedAt: Date?
    var lastBatchSampleCount: Int?
    var audit: [SyncAuditEntry] = []

    static let empty = DashboardSnapshot()
}

enum SyncPhase: Equatable, Sendable {
    case idle, collecting, committing, finished, failed

    var isWorking: Bool { self == .collecting || self == .committing }
}

enum CloudDisplayState: Equatable, Sendable {
    case localOnly
    case localSaved
    case pairingRequired
    case queued
    case uploading
    case cloudSaved
    case metricsPending
    case metricsCurrent
    case erasurePending
    case activeErasureConfirmed
    case offline
    case failure(String)
}

/// An app coordinator supplies callbacks and changes these values after real work completes.
@MainActor
final class HealthDashboardModel: ObservableObject {
    @Published var snapshot: DashboardSnapshot = .empty
    @Published var syncPhase: SyncPhase = .idle
    @Published var cloudState: CloudDisplayState = .localOnly
    @Published var uploadConsentGranted = false
    @Published var isPaired = false
    @Published var isWorking = false
    @Published var errorMessage: String?

    var onSync: (() async -> Void)?
    var onRequestHealthAccess: (() async -> Void)?
    var onPair: ((URL, String) async -> Void)?
    var onSetUploadConsent: ((Bool) async -> Void)?
    var onStopUpload: (() async -> Void)?
    var onEraseCloud: (() async -> Void)?
    var onRefresh: (() async -> Void)?

    init(snapshot: DashboardSnapshot = .empty) {
        self.snapshot = snapshot
    }
}
