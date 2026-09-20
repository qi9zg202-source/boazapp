import Foundation
import XCTest
@testable import boazapp

final class SleepVitalWindowTests: XCTestCase {
    func testOvernightAverageUsesCompleteIntervalDespiteLaterHistory() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let database = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let end = start.addingTimeInterval(8 * 3600)
        let type = "HKQuantityTypeIdentifierHeartRate"
        func reading(_ id: String, at date: Date, value: Double?, kind: String? = nil) -> HealthEvent {
            HealthEvent(eventID: id, revision: 0, operation: "upsert", kind: "quantity", type: kind ?? type,
                        sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                        startUTC: date, endUTC: date, value: value, unit: "count/min", metadata: [:])
        }

        // More than 10,000 readings inside the night, plus enough later readings
        // to displace the whole night under the former descending LIMIT query.
        for page in 0..<21 {
            let range = (page * 500)..<min((page + 1) * 500, 10_001)
            let inNight = range.map { reading("night-\($0)", at: start.addingTimeInterval(Double($0)), value: 60) }
            let later = range.map { reading("later-\($0)", at: end.addingTimeInterval(Double($0 + 1)), value: 150) }
            _ = try await database.apply(events: inNight + later)
        }
        _ = try await database.apply(events: [
            reading("at-end", at: end, value: 80),
            reading("before", at: start.addingTimeInterval(-1), value: 180),
            reading("missing", at: start, value: nil),
            reading("other-type", at: start, value: 999, kind: "other"),
            reading("deleted", at: start, value: 200)
        ])
        _ = try await database.apply(events: [
            HealthEvent(eventID: "deleted", revision: 0, operation: "delete", kind: "quantity", type: type,
                        sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                        value: nil, unit: nil, metadata: [:])
        ])
        let average = try await database.averageValue(typeIdentifier: type, from: start, through: end)
        XCTAssertEqual(try XCTUnwrap(average), (10_001.0 * 60 + 80) / 10_002.0, accuracy: 0.000001)
        let absent = try await database.averageValue(typeIdentifier: "absent", from: start, through: end)
        XCTAssertNil(absent)
    }

    func testMeasuredZeroIsNotMissingAndWindowIsInclusive() async throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let database = try BoazLocalDatabase(url: folder.appendingPathComponent("health.sqlite"))
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        _ = try await database.apply(events: [
            HealthEvent(eventID: "zero", revision: 0, operation: "upsert", kind: "quantity", type: "synthetic",
                        sourceBundleID: "synthetic", sourceName: "Synthetic", startUTC: date, endUTC: date,
                        value: 0, unit: "count", metadata: [:])
        ])
        let measured = try await database.averageValue(typeIdentifier: "synthetic", from: date, through: date)
        XCTAssertEqual(measured, 0)
        let empty = try await database.averageValue(typeIdentifier: "synthetic", from: date.addingTimeInterval(1), through: date.addingTimeInterval(2))
        XCTAssertNil(empty)
    }
}
