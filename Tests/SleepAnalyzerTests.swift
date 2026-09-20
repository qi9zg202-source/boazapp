import Foundation
import HealthKit
import XCTest
@testable import boazapp

final class SleepAnalyzerTests: XCTestCase {
    private let origin = Date(timeIntervalSince1970: 1_700_000_000)

    private func segment(_ startMinutes: Double, _ endMinutes: Double,
                         _ stage: SleepStage, source: String = "watch") -> SleepSegment {
        SleepSegment(start: origin.addingTimeInterval(startMinutes * 60),
                     end: origin.addingTimeInterval(endMinutes * 60),
                     stage: stage, sourceID: source)
    }

    func testOverlapsCountEachMinuteOnlyOnce() {
        let samples = [
            segment(0, 120, .inBed),
            segment(0, 60, .asleepCore),
            segment(30, 90, .asleepDeep),
            segment(90, 120, .awake)
        ]
        let session = try! XCTUnwrap(SleepAnalyzer.sessions(from: samples).first)
        XCTAssertEqual(session.stages.coreSeconds, 30 * 60)
        XCTAssertEqual(session.stages.deepSeconds, 60 * 60)
        XCTAssertEqual(session.stages.awakeSeconds, 30 * 60)
        XCTAssertEqual(session.asleepSeconds, 90 * 60)
        XCTAssertEqual(session.inBedSeconds, 120 * 60)
        XCTAssertEqual(session.efficiency, 0.75)
    }

    func testDetailedSourceWinsUnspecifiedOverlap() {
        let samples = [
            segment(0, 120, .asleepUnspecified, source: "phone"),
            segment(0, 60, .asleepCore),
            segment(60, 120, .asleepREM)
        ]
        let session = try! XCTUnwrap(SleepAnalyzer.sessions(from: samples).first)
        XCTAssertEqual(session.stages.coreSeconds, 60 * 60)
        XCTAssertEqual(session.stages.remSeconds, 60 * 60)
        XCTAssertEqual(session.stages.unspecifiedSeconds, 0)
        XCTAssertNil(session.efficiency)
    }

    func testEqualCoverageUsesStableSourceTieBreak() {
        let samples = [
            segment(0, 60, .asleepDeep, source: "z-source"),
            segment(0, 60, .asleepCore, source: "a-source")
        ]
        let session = try! XCTUnwrap(SleepAnalyzer.sessions(from: samples).first)
        XCTAssertEqual(session.stages.coreSeconds, 3600)
        XCTAssertEqual(session.stages.deepSeconds, 0)
    }

    func testTwoNightsAndNapAreSeparate() {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let first = segment(0, 420, .asleepCore)
        let second = segment(24 * 60, 24 * 60 + 420, .asleepCore)
        let nap = segment(24 * 60 + 720, 24 * 60 + 780, .asleepCore)
        let sessions = SleepAnalyzer.sessions(from: [first, second, nap], calendar: calendar)
        XCTAssertEqual(sessions.count, 3)
        XCTAssertEqual(sessions.filter(\.isNap).count, 1)
        XCTAssertEqual(sessions.reduce(0) { $0 + $1.asleepSeconds }, 15 * 3600)
    }

    func testLatestCompletedDoesNotUseOpenSession() {
        let old = segment(0, 360, .asleepCore)
        let recent = segment(600, 720, .asleepCore)
        let sessions = SleepAnalyzer.sessions(from: [old, recent])
        let now = origin.addingTimeInterval(725 * 60)
        let latest = SleepAnalyzer.latestCompletedSession(from: sessions, now: now)
        XCTAssertEqual(latest?.start, old.start)
    }

    func testDeleteAndBadStageAreIgnored() {
        let start = origin, end = origin.addingTimeInterval(3600)
        let deleted = HealthEvent(eventID: "x", revision: 2, operation: "delete", kind: "category",
                                  type: HKCategoryTypeIdentifier.sleepAnalysis.rawValue, sourceBundleID: "",
                                  sourceName: "", startUTC: nil, endUTC: nil, value: nil, unit: nil, metadata: [:])
        let bad = HealthEvent(eventID: "y", revision: 1, operation: "upsert", kind: "category",
                              type: HKCategoryTypeIdentifier.sleepAnalysis.rawValue, sourceBundleID: "watch",
                              sourceName: "Watch", startUTC: start, endUTC: end,
                              value: 99, unit: "category-value", metadata: [:])
        XCTAssertTrue(SleepAnalyzer.segments(from: [deleted, bad]).isEmpty)
    }
}
