import HealthKit
import XCTest
@testable import boazapp

final class HealthUnitTests: XCTestCase {
    private func assertConversion(_ type: HKQuantityTypeIdentifier, _ raw: Double, _ unit: HKUnit,
                                  expected: Double, label: String, file: StaticString = #filePath, line: UInt = #line) throws {
        let conversion = try XCTUnwrap(HealthTypeCatalog.canonicalUnit(for: type.rawValue), file: file, line: line)
        let quantity = HKQuantity(unit: unit, doubleValue: raw)
        XCTAssertTrue(quantity.is(compatibleWith: conversion.unit), file: file, line: line)
        XCTAssertEqual(quantity.doubleValue(for: conversion.unit) * conversion.multiplier, expected, accuracy: 1e-7, file: file, line: line)
        XCTAssertEqual(conversion.label, label, file: file, line: line)
    }

    func testPercentagesAndHRVConvertOnce() throws {
        try assertConversion(.oxygenSaturation, 0.98, .percent(), expected: 98, label: "%")
        try assertConversion(.bodyFatPercentage, 0.20, .percent(), expected: 20, label: "%")
        try assertConversion(.heartRateVariabilitySDNN, 0.05, .second(), expected: 50, label: "ms")
    }

    func testMassDistanceAndEnergyConversions() throws {
        try assertConversion(.bodyMass, 150, .pound(), expected: 68.0388555, label: "kg")
        for type: HKQuantityTypeIdentifier in [.distanceWalkingRunning, .distanceCycling, .distanceSwimming] {
            try assertConversion(type, 1, .mile(), expected: 1609.344, label: "m")
        }
        try assertConversion(.activeEnergyBurned, 100, .kilocalorie(), expected: 100, label: "kcal")
    }

    func testWristTemperatureIsAbsoluteCelsius() throws {
        try assertConversion(.appleSleepingWristTemperature, 97.25, .degreeFahrenheit(), expected: 36.25, label: "degC")
        try assertConversion(.appleSleepingWristTemperature, 36.25, .degreeCelsius(), expected: 36.25, label: "degC")
        try assertConversion(.appleSleepingWristTemperature, 0, .degreeCelsius(), expected: 0, label: "degC")
    }

    func testCountsPressureAndTimeHaveExplicitUnits() throws {
        for type: HKQuantityTypeIdentifier in [.heartRate, .restingHeartRate, .respiratoryRate] {
            try assertConversion(type, 1.2, .count().unitDivided(by: .second()), expected: 72, label: "count/min")
        }
        for type: HKQuantityTypeIdentifier in [.bloodPressureSystolic, .bloodPressureDiastolic] {
            try assertConversion(type, 120, .millimeterOfMercury(), expected: 120, label: "mmHg")
        }
        for type: HKQuantityTypeIdentifier in [.stepCount, .flightsClimbed, .bodyMassIndex] {
            try assertConversion(type, 42, .count(), expected: 42, label: "count")
        }
        try assertConversion(.appleExerciseTime, 3600, .second(), expected: 60, label: "min")
        XCTAssertNil(HealthTypeCatalog.canonicalUnit(for: "unknown-type"))
    }

    @MainActor
    func testActivitySummaryPredicateHasCalendarAcrossYearBoundary() throws {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = try XCTUnwrap(TimeZone(identifier: "Asia/Shanghai"))
        let start = try XCTUnwrap(calendar.date(from: DateComponents(year: 2013, month: 12, day: 31)))
        let end = try XCTUnwrap(calendar.date(from: DateComponents(year: 2014, month: 1, day: 30)))

        let (first, last) = try HealthKitManager.activitySummaryDateComponents(
            from: start, through: end, calendar: calendar
        )
        XCTAssertEqual(first.calendar?.identifier, .gregorian)
        XCTAssertEqual(last.calendar?.identifier, .gregorian)
        XCTAssertEqual(first.calendar?.timeZone, calendar.timeZone)
        XCTAssertEqual(last.calendar?.timeZone, calendar.timeZone)
        XCTAssertEqual(first.era, 1)
        XCTAssertEqual(last.era, 1)
        XCTAssertEqual(first.year, 2013)
        XCTAssertEqual(first.month, 12)
        XCTAssertEqual(first.day, 31)
        XCTAssertEqual(last.year, 2014)
        XCTAssertEqual(last.month, 1)
        XCTAssertEqual(last.day, 30)
        XCTAssertNotNil(HKQuery.predicate(forActivitySummariesBetweenStart: first, end: last))

        let outsideRange = try XCTUnwrap(calendar.date(from: DateComponents(year: 2014, month: 1, day: 31)))
        XCTAssertThrowsError(try HealthKitManager.activitySummaryDateComponents(
            from: start, through: outsideRange, calendar: calendar
        ))

        var buddhistCalendar = Calendar(identifier: .buddhist)
        buddhistCalendar.timeZone = calendar.timeZone
        let (gregorianFirst, gregorianLast) = try HealthKitManager.activitySummaryDateComponents(
            from: start, through: end, calendar: buddhistCalendar
        )
        XCTAssertEqual(gregorianFirst.calendar?.identifier, .gregorian)
        XCTAssertEqual(gregorianLast.calendar?.identifier, .gregorian)
        XCTAssertEqual(gregorianFirst.year, 2013)
        XCTAssertEqual(gregorianLast.year, 2014)
    }
}
