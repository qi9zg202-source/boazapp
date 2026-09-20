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
        for input in [Array(samples.reversed()), samples + samples] {
            let repeated = try! XCTUnwrap(SleepAnalyzer.sessions(from: input).first)
            XCTAssertEqual(repeated.stages, session.stages)
            XCTAssertEqual(repeated.inBedSeconds, session.inBedSeconds)
        }
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
        let reversed = try! XCTUnwrap(SleepAnalyzer.sessions(from: Array(samples.reversed())).first)
        XCTAssertEqual(reversed.stages, session.stages)
    }

    func testEqualCoverageUsesStableSourceTieBreak() {
        let samples = [
            segment(0, 60, .asleepDeep, source: "z-source"),
            segment(0, 60, .asleepCore, source: "a-source")
        ]
        let session = try! XCTUnwrap(SleepAnalyzer.sessions(from: samples).first)
        XCTAssertEqual(session.stages.coreSeconds, 3600)
        XCTAssertEqual(session.stages.deepSeconds, 0)
        let reversed = try! XCTUnwrap(SleepAnalyzer.sessions(from: Array(samples.reversed())).first)
        XCTAssertEqual(reversed.stages, session.stages)
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
        for value in [Double.nan, Double.infinity, 1e300, -1, 2.5] {
            let invalid = HealthEvent(eventID: "invalid", revision: 1, operation: "upsert", kind: "category",
                                      type: HKCategoryTypeIdentifier.sleepAnalysis.rawValue, sourceBundleID: "watch",
                                      sourceName: "Watch", startUTC: start, endUTC: end,
                                      value: value, unit: "category-value", metadata: [:])
            XCTAssertTrue(SleepAnalyzer.segments(from: [invalid]).isEmpty)
        }
    }

    func testAwakePriorityAndPartialInBedIntersection() throws {
        let conflicts = [segment(0, 120, .asleepDeep), segment(30, 60, .awake)]
        let resolved = try XCTUnwrap(SleepAnalyzer.sessions(from: conflicts).first)
        XCTAssertEqual(resolved.stages.deepSeconds, 90 * 60)
        XCTAssertEqual(resolved.stages.awakeSeconds, 30 * 60)
        let higherSource = try XCTUnwrap(SleepAnalyzer.sessions(from: conflicts + [segment(0, 120, .asleepREM, source: "a-source")]).first)
        XCTAssertEqual(higherSource.stages.remSeconds, 120 * 60)
        XCTAssertEqual(higherSource.stages.awakeSeconds, 0)
        let partial = [segment(0, 60, .inBed), segment(30, 90, .inBed), segment(60, 180, .asleepCore)]
        let session = try XCTUnwrap(SleepAnalyzer.sessions(from: partial).first)
        XCTAssertEqual(session.inBedSeconds, 90 * 60)
        XCTAssertEqual(session.asleepSeconds, 120 * 60)
        XCTAssertEqual(try XCTUnwrap(session.efficiency), 1.0 / 3.0, accuracy: 1e-12)
    }

    func testInvalidDurationsAndExactSessionGap() {
        let invalid = [segment(5, 5, .asleepCore), segment(10, 5, .asleepDeep), segment(0, 1441, .asleepREM)]
        XCTAssertTrue(SleepAnalyzer.sessions(from: invalid).isEmpty)
        XCTAssertEqual(SleepAnalyzer.sessions(from: [segment(0, 60, .asleepCore), segment(150, 210, .asleepDeep)]).count, 1)
        XCTAssertEqual(SleepAnalyzer.sessions(from: [segment(0, 60, .asleepCore), segment(150 + 1.0 / 60, 210, .asleepDeep)]).count, 2)
        let recent = SleepAnalyzer.sessions(from: [segment(0, 360, .asleepCore)])
        XCTAssertNil(SleepAnalyzer.latestCompletedSession(from: recent, now: origin.addingTimeInterval(369 * 60)))
        XCTAssertNotNil(SleepAnalyzer.latestCompletedSession(from: recent, now: origin.addingTimeInterval(370 * 60)))
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "America/New_York")!
        let start = ISO8601DateFormatter().date(from: "2026-03-08T06:30:00Z")!
        let end = ISO8601DateFormatter().date(from: "2026-03-08T07:30:00Z")!
        let daylightSaving = SleepAnalyzer.sessions(from: [SleepSegment(start: start, end: end, stage: .asleepCore, sourceID: "watch")], calendar: calendar)
        XCTAssertEqual(calendar.component(.hour, from: start), 1)
        XCTAssertEqual(calendar.component(.hour, from: end), 3)
        XCTAssertEqual(daylightSaving.first?.asleepSeconds, 3600)
    }
}
