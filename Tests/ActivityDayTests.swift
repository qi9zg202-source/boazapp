import Foundation
import HealthKit
import XCTest
@testable import boazapp

final class ActivityDayTests: XCTestCase {
    private func date(_ timestamp: String) throws -> Date {
        try XCTUnwrap(ISO8601DateFormatter().date(from: timestamp))
    }

    private func calendar(_ zone: String, identifier: Calendar.Identifier = .gregorian) throws -> Calendar {
        var calendar = Calendar(identifier: identifier)
        calendar.timeZone = try XCTUnwrap(TimeZone(identifier: zone))
        return calendar
    }

    @MainActor
    func testBothBoundsAttachGregorianCalendarAndConstructRealHealthKitPredicate() throws {
        let requested = try calendar("Pacific/Honolulu", identifier: .buddhist)
        let (first, last) = try HealthKitManager.activitySummaryDateComponents(
            from: date("2026-09-18T09:30:00Z"), through: date("2026-09-19T10:30:00Z"), calendar: requested)
        for component in [first, last] {
            XCTAssertEqual(component.calendar?.identifier, .gregorian)
            XCTAssertEqual(component.calendar?.timeZone, requested.timeZone)
            XCTAssertEqual(component.era, 1)
            XCTAssertEqual(component.year, 2026)
            XCTAssertEqual(component.month, 9)
        }
        XCTAssertEqual(first.day, 17)
        XCTAssertEqual(last.day, 19)
        // Construct the actual predicate, without creating a store or executing a query.
        let predicate = HKQuery.predicate(forActivitySummariesBetweenStart: first, end: last)
        XCTAssertNotNil(predicate)
    }

    @MainActor
    func testLocalDateLabelsAcrossDSTAndLeapDayUseTheExplicitZone() throws {
        let fixtures: [(String, String, String, [Int], [Int])] = [
            ("America/New_York", "2026-03-08T04:30:00Z", "2026-03-09T04:30:00Z", [2026, 3, 7], [2026, 3, 9]),
            ("America/New_York", "2026-11-01T03:30:00Z", "2026-11-02T05:30:00Z", [2026, 10, 31], [2026, 11, 2]),
            ("Asia/Tokyo", "2024-02-28T15:00:00Z", "2024-02-29T15:00:00Z", [2024, 2, 29], [2024, 3, 1]),
            ("Pacific/Honolulu", "2024-03-01T09:30:00Z", "2024-03-01T10:30:00Z", [2024, 2, 29], [2024, 3, 1])
        ]
        for (zone, start, end, expectedFirst, expectedLast) in fixtures {
            let explicitCalendar = try calendar(zone)
            let (first, last) = try HealthKitManager.activitySummaryDateComponents(
                from: date(start), through: date(end), calendar: explicitCalendar)
            XCTAssertEqual([first.year, first.month, first.day].compactMap { $0 }, expectedFirst, zone)
            XCTAssertEqual([last.year, last.month, last.day].compactMap { $0 }, expectedLast, zone)
            XCTAssertEqual(first.calendar?.timeZone, explicitCalendar.timeZone)
            XCTAssertEqual(last.calendar?.timeZone, explicitCalendar.timeZone)
            XCTAssertNotNil(HKQuery.predicate(forActivitySummariesBetweenStart: first, end: last))
        }
    }

    @MainActor
    func testThirtyDaySpanIsAllowedButReversedAndLongerRangesAreRejected() throws {
        let explicitCalendar = try calendar("America/New_York")
        let start = try date("2026-03-01T05:00:00Z") // Local March 1 midnight, before DST.
        let end = try date("2026-03-31T04:00:00Z") // Local March 31 midnight, after DST.
        let (first, last) = try HealthKitManager.activitySummaryDateComponents(
            from: start, through: end, calendar: explicitCalendar)
        XCTAssertEqual(first.day, 1)
        XCTAssertEqual(last.day, 31)
        XCTAssertEqual(end.timeIntervalSince(start), 30 * 86_400 - 3600)
        let sameDay = try HealthKitManager.activitySummaryDateComponents(
            from: start, through: start, calendar: explicitCalendar)
        XCTAssertEqual(sameDay.0, sameDay.1)
        let tooLate = try date("2026-04-01T04:00:00Z")
        for (invalidStart, invalidEnd) in [(end, start), (start, tooLate), (start.addingTimeInterval(1), start)] {
            XCTAssertThrowsError(try HealthKitManager.activitySummaryDateComponents(
                from: invalidStart, through: invalidEnd, calendar: explicitCalendar)) { error in
                guard case HealthCollectionError.invalidDateRange = error else {
                    return XCTFail("Expected invalidDateRange, received \(error)")
                }
            }
        }
    }
}
