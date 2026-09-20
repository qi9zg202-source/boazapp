import HealthKit

/// Deliberately limited to the data named in the product contract. HealthKit may return
/// no records for a type even after an authorization request succeeds.
enum HealthTypeCatalog {
    static let pageLimit = 500

    static let quantityTypes: [HKQuantityTypeIdentifier] = [
        .appleSleepingWristTemperature,
        .heartRate,
        .restingHeartRate,
        .respiratoryRate,
        .oxygenSaturation,
        .heartRateVariabilitySDNN,
        .bloodPressureSystolic,
        .bloodPressureDiastolic,
        .bodyMass,
        .bodyFatPercentage,
        .bodyMassIndex,
        .stepCount,
        .flightsClimbed,
        .activeEnergyBurned,
        .appleExerciseTime,
        .distanceWalkingRunning,
        .distanceCycling,
        .distanceSwimming
    ]

    static let categoryTypes: [HKCategoryTypeIdentifier] = [
        .sleepAnalysis,
        .appleStandHour
    ]

    static var sampleTypes: [HKSampleType] {
        let quantities = quantityTypes.compactMap(HKObjectType.quantityType(forIdentifier:))
        let categories = categoryTypes.compactMap(HKObjectType.categoryType(forIdentifier:))
        return quantities + categories + [HKObjectType.workoutType()]
    }

    static var readTypes: Set<HKObjectType> {
        Set(sampleTypes.map { $0 as HKObjectType } + [HKObjectType.activitySummaryType()])
    }

    static func sampleType(for identifier: String) -> HKSampleType? {
        sampleTypes.first { $0.identifier == identifier }
    }

    static var sampleTypeIdentifiers: [String] {
        sampleTypes.map(\.identifier)
    }

    /// HealthKit quantities do not expose the source app's original display unit.
    /// These canonical units make values comparable and the output unit explicit.
    static func canonicalUnit(for identifier: String) -> (unit: HKUnit, label: String, multiplier: Double)? {
        let bpm = HKUnit.count().unitDivided(by: .minute())
        switch identifier {
        case HKQuantityTypeIdentifier.appleSleepingWristTemperature.rawValue:
            return (.degreeCelsius(), "degC", 1)
        case HKQuantityTypeIdentifier.heartRate.rawValue,
             HKQuantityTypeIdentifier.restingHeartRate.rawValue,
             HKQuantityTypeIdentifier.respiratoryRate.rawValue:
            return (bpm, "count/min", 1)
        case HKQuantityTypeIdentifier.oxygenSaturation.rawValue,
             HKQuantityTypeIdentifier.bodyFatPercentage.rawValue:
            return (.percent(), "%", 100)
        case HKQuantityTypeIdentifier.heartRateVariabilitySDNN.rawValue:
            return (.secondUnit(with: .milli), "ms", 1)
        case HKQuantityTypeIdentifier.bloodPressureSystolic.rawValue,
             HKQuantityTypeIdentifier.bloodPressureDiastolic.rawValue:
            return (.millimeterOfMercury(), "mmHg", 1)
        case HKQuantityTypeIdentifier.bodyMass.rawValue:
            return (.gramUnit(with: .kilo), "kg", 1)
        case HKQuantityTypeIdentifier.bodyMassIndex.rawValue:
            return (.count(), "count", 1)
        case HKQuantityTypeIdentifier.stepCount.rawValue,
             HKQuantityTypeIdentifier.flightsClimbed.rawValue:
            return (.count(), "count", 1)
        case HKQuantityTypeIdentifier.activeEnergyBurned.rawValue:
            return (.kilocalorie(), "kcal", 1)
        case HKQuantityTypeIdentifier.appleExerciseTime.rawValue:
            return (.minute(), "min", 1)
        case HKQuantityTypeIdentifier.distanceWalkingRunning.rawValue,
             HKQuantityTypeIdentifier.distanceCycling.rawValue,
             HKQuantityTypeIdentifier.distanceSwimming.rawValue:
            return (.meter(), "m", 1)
        default:
            return nil
        }
    }
}
