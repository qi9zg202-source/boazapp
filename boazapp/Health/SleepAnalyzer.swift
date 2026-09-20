import Foundation
import HealthKit

enum SleepStage: Int, Sendable {
    case inBed = 0
    case asleepUnspecified = 1
    case awake = 2
    case asleepCore = 3
    case asleepDeep = 4
    case asleepREM = 5

    var isAsleep: Bool {
        switch self {
        case .asleepUnspecified, .asleepCore, .asleepDeep, .asleepREM: true
        case .inBed, .awake: false
        }
    }
}

struct SleepSegment: Sendable, Equatable {
    let start: Date
    let end: Date
    let stage: SleepStage
    let sourceID: String
}

struct SleepStageTotals: Sendable, Equatable {
    var coreSeconds: TimeInterval = 0
    var deepSeconds: TimeInterval = 0
    var remSeconds: TimeInterval = 0
    var awakeSeconds: TimeInterval = 0
    var unspecifiedSeconds: TimeInterval = 0

    var asleepSeconds: TimeInterval { coreSeconds + deepSeconds + remSeconds + unspecifiedSeconds }
}

struct SleepSession: Sendable, Equatable, Identifiable {
    var id: Date { start }
    let start: Date
    let end: Date
    let asleepStart: Date?
    let asleepEnd: Date?
    let inBedSeconds: TimeInterval
    let stages: SleepStageTotals
    let sampleCount: Int
    let isNap: Bool
    /// Nil when there is no in-bed interval; denominator is observed in-bed time.
    let efficiency: Double?

    var asleepSeconds: TimeInterval { stages.asleepSeconds }
}

/// A deterministic interval sweep. For overlapping devices, the source with the most
/// detailed stage coverage in the session wins; ties use total coverage then source ID.
/// Awake wins conflicting stages within that source. In-bed is measured independently.
enum SleepAnalyzer {
    static let sessionGap: TimeInterval = 90 * 60

    static func sessions(from input: [SleepSegment], calendar: Calendar = .current) -> [SleepSession] {
        let valid = input.filter { $0.start < $0.end && $0.end.timeIntervalSince($0.start) <= 24 * 3600 }
            .sorted { $0.start == $1.start ? $0.end < $1.end : $0.start < $1.start }
        guard !valid.isEmpty else { return [] }

        var groups: [[SleepSegment]] = []
        var current: [SleepSegment] = []
        var groupEnd = Date.distantPast
        for segment in valid {
            if !current.isEmpty && segment.start.timeIntervalSince(groupEnd) > sessionGap {
                groups.append(current)
                current = []
            }
            current.append(segment)
            groupEnd = max(groupEnd, segment.end)
        }
        if !current.isEmpty { groups.append(current) }
        return groups.map { makeSession($0, calendar: calendar) }.sorted { $0.start > $1.start }
    }

    static func latestCompletedSession(from sessions: [SleepSession], now: Date = Date()) -> SleepSession? {
        let completed = sessions.filter { $0.end <= now.addingTimeInterval(-10 * 60) && $0.asleepSeconds > 0 }
            .sorted { $0.end > $1.end }
        return completed.first { !$0.isNap } ?? completed.first
    }

    static func segments(from events: [HealthEvent]) -> [SleepSegment] {
        events.compactMap { event in
            guard event.operation == "upsert",
                  event.type == HKCategoryTypeIdentifier.sleepAnalysis.rawValue,
                  let start = event.startUTC, let end = event.endUTC,
                  let value = event.value, value.isFinite, (0...5).contains(value),
                  let stage = SleepStage(rawValue: Int(value)),
                  Double(stage.rawValue) == value else { return nil }
            return SleepSegment(start: start, end: end, stage: stage, sourceID: event.sourceBundleID)
        }
    }

    private static func makeSession(_ segments: [SleepSegment], calendar: Calendar) -> SleepSession {
        let start = segments.map(\.start).min()!
        let end = segments.map(\.end).max()!
        let inBed = mergedIntervals(segments.filter { $0.stage == .inBed }.map { DateInterval(start: $0.start, end: $0.end) })
        let inBedSeconds = inBed.reduce(0) { $0 + $1.duration }
        let stages = segments.filter { $0.stage != .inBed }
        let sourceOrder = sourceRanking(stages)
        let points = Array(Set(stages.flatMap { [$0.start, $0.end] })).sorted()
        var totals = SleepStageTotals()
        var asleepStart: Date?
        var asleepEnd: Date?
        var asleepInBed: TimeInterval = 0

        if points.count > 1 {
            for i in 0..<(points.count - 1) {
                let a = points[i], b = points[i + 1]
                guard a < b else { continue }
                let active = stages.filter { $0.start < b && $0.end > a }
                guard let chosen = active.min(by: { lhs, rhs in
                    let lr = sourceOrder[lhs.sourceID] ?? Int.max
                    let rr = sourceOrder[rhs.sourceID] ?? Int.max
                    return lr == rr ? stagePriority(lhs.stage) > stagePriority(rhs.stage) : lr < rr
                }) else { continue }
                let duration = b.timeIntervalSince(a)
                switch chosen.stage {
                case .asleepCore: totals.coreSeconds += duration
                case .asleepDeep: totals.deepSeconds += duration
                case .asleepREM: totals.remSeconds += duration
                case .asleepUnspecified: totals.unspecifiedSeconds += duration
                case .awake: totals.awakeSeconds += duration
                case .inBed: break
                }
                if chosen.stage.isAsleep {
                    asleepStart = min(asleepStart ?? a, a)
                    asleepEnd = max(asleepEnd ?? b, b)
                    for interval in inBed {
                        asleepInBed += max(0, min(b, interval.end).timeIntervalSince(max(a, interval.start)))
                    }
                }
            }
        }
        let hour = calendar.component(.hour, from: asleepStart ?? start)
        let isNap = totals.asleepSeconds < 3 * 3600 && (9..<19).contains(hour)
        return SleepSession(start: start, end: end, asleepStart: asleepStart, asleepEnd: asleepEnd,
                            inBedSeconds: inBedSeconds, stages: totals, sampleCount: segments.count,
                            isNap: isNap, efficiency: inBedSeconds > 0 ? min(1, asleepInBed / inBedSeconds) : nil)
    }

    private static func sourceRanking(_ segments: [SleepSegment]) -> [String: Int] {
        let sources = Set(segments.map(\.sourceID))
        let sorted = sources.sorted { lhs, rhs in
            let l = segments.filter { $0.sourceID == lhs }
            let r = segments.filter { $0.sourceID == rhs }
            let ld = coveredDuration(l.filter { $0.stage != .asleepUnspecified })
            let rd = coveredDuration(r.filter { $0.stage != .asleepUnspecified })
            if ld != rd { return ld > rd }
            let lt = coveredDuration(l), rt = coveredDuration(r)
            if lt != rt { return lt > rt }
            return lhs < rhs
        }
        return Dictionary(uniqueKeysWithValues: sorted.enumerated().map { ($0.element, $0.offset) })
    }

    private static func coveredDuration(_ segments: [SleepSegment]) -> TimeInterval {
        mergedIntervals(segments.map { DateInterval(start: $0.start, end: $0.end) })
            .reduce(0) { $0 + $1.duration }
    }

    private static func stagePriority(_ stage: SleepStage) -> Int {
        switch stage {
        case .awake: 5
        case .asleepDeep: 4
        case .asleepREM: 3
        case .asleepCore: 2
        case .asleepUnspecified: 1
        case .inBed: 0
        }
    }

    private static func mergedIntervals(_ intervals: [DateInterval]) -> [DateInterval] {
        var result: [DateInterval] = []
        for interval in intervals.sorted(by: { $0.start < $1.start }) {
            if let last = result.last, interval.start <= last.end {
                result[result.count - 1] = DateInterval(start: last.start, end: max(last.end, interval.end))
            } else {
                result.append(interval)
            }
        }
        return result
    }
}
