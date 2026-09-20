import Foundation
@preconcurrency import HealthKit

/// HealthKit explicitly permits invoking its observer completion after asynchronous work.
/// The imported block lacks a Sendable annotation, so carry only that block across Task.
private struct ObserverCompletion: @unchecked Sendable {
    let body: () -> Void
    func finish() { body() }
}

/// Reads only the allowlisted types. A successful authorization request is not proof that
/// HealthKit granted read access; callers must represent empty pages as "no readable data".
@MainActor
final class HealthKitManager {
    private let store = HKHealthStore()
    private var observerQueries: [HKObserverQuery] = []

    var isAvailable: Bool { HKHealthStore.isHealthDataAvailable() }
    var typeIdentifiers: [String] { HealthTypeCatalog.sampleTypeIdentifiers }

    func requestReadAccess() async throws {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            store.requestAuthorization(toShare: [], read: HealthTypeCatalog.readTypes) { completed, error in
                if let error { continuation.resume(throwing: error) }
                else if completed { continuation.resume() }
                else { continuation.resume(throwing: HealthCollectionError.authorizationRequestFailed) }
            }
        }
    }

    /// A nil anchor starts a full readable-history import. Repeated calls with the returned
    /// anchor drain the bounded history and later retrieve changes, including deletions.
    /// The caller owns the transaction that pairs each page with its new anchor.
    func collectPage(typeIdentifier: String, anchorData: Data?) async throws -> HealthImportPage {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        guard let sampleType = HealthTypeCatalog.sampleType(for: typeIdentifier) else {
            throw HealthCollectionError.unsupportedType(typeIdentifier)
        }
        let anchor = try decodeAnchor(anchorData)
        let result = try await anchoredPage(sampleType: sampleType, predicate: nil, anchor: anchor)
        let converted = try result.samples.map { sample in
            guard let converted = event(from: sample) else {
                throw HealthCollectionError.unconvertibleSample(sample.uuid.uuidString)
            }
            return converted
        }
        let events = converted + result.deleted.map {
            HealthEvent(eventID: $0.uuid.uuidString, revision: 2, operation: "delete",
                        kind: kind(for: sampleType), type: typeIdentifier,
                        sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                        value: nil, unit: nil, metadata: [:])
        }
        return HealthImportPage(
            typeIdentifier: typeIdentifier,
            events: events,
            nextAnchorData: try encodeAnchor(result.anchor),
            countReachedLimit: result.samples.count + result.deleted.count >= HealthTypeCatalog.pageLimit
        )
    }

    /// Query date bounds in the user's current calendar. Each day produces the three ring
    /// actual values and goals. Re-query recent days because summaries can be revised later.
    /// The local ledger must assign monotonically increasing revisions on changed day values.
    func fetchActivitySummaries(from start: Date, through end: Date) async throws -> [HealthEvent] {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        guard start <= end else { throw HealthCollectionError.invalidDateRange }
        let calendar = Calendar.current
        guard let dayCount = calendar.dateComponents([.day], from: calendar.startOfDay(for: start),
                                                     to: calendar.startOfDay(for: end)).day,
              dayCount <= 30 else { throw HealthCollectionError.invalidDateRange }
        let first = calendar.dateComponents([.year, .month, .day], from: start)
        let last = calendar.dateComponents([.year, .month, .day], from: end)
        let predicate = HKQuery.predicate(forActivitySummariesBetweenStart: first, end: last)
        let summaries = try await HKActivitySummaryQueryDescriptor(predicate: predicate).result(for: store)
        return try summaries.flatMap(activityEvents)
    }

    /// HealthKit's statistics engine supplies day totals without summing overlapping
    /// phone/watch sample rows in the app. Nil is unknown, not zero.
    func fetchTodayCumulative(now: Date = Date()) async throws -> TodayCumulative {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        let start = Calendar.current.startOfDay(for: now)
        let measures: [(HKQuantityTypeIdentifier, HKUnit, Double)] = [
            (.stepCount, .count(), 1),
            (.flightsClimbed, .count(), 1),
            (.activeEnergyBurned, .kilocalorie(), 1),
            (.appleExerciseTime, .minute(), 1)
        ]
        var values: [String: Double] = [:]
        var failed: [String] = []
        for (identifier, unit, multiplier) in measures {
            guard let type = HKObjectType.quantityType(forIdentifier: identifier) else {
                failed.append(identifier.rawValue)
                continue
            }
            let predicate = HKQuery.predicateForSamples(withStart: start, end: now, options: .strictStartDate)
            let descriptor = HKStatisticsQueryDescriptor(
                predicate: .quantitySample(type: type, predicate: predicate), options: .cumulativeSum
            )
            do {
                if let quantity = try await descriptor.result(for: store)?.sumQuantity() {
                    let value = quantity.doubleValue(for: unit) * multiplier
                    if value.isFinite { values[identifier.rawValue] = value }
                }
            } catch {
                failed.append(identifier.rawValue)
            }
        }
        return TodayCumulative(dayStart: start, asOf: now,
                               steps: values[HKQuantityTypeIdentifier.stepCount.rawValue],
                               flights: values[HKQuantityTypeIdentifier.flightsClimbed.rawValue],
                               activeEnergyKcal: values[HKQuantityTypeIdentifier.activeEnergyBurned.rawValue],
                               exerciseMinutes: values[HKQuantityTypeIdentifier.appleExerciseTime.rawValue],
                               failedTypes: failed)
    }

    /// HKWorkoutEvent has no sample UUID. Its stable position within an immutable workout
    /// is used as the derived identity. The offset allows bounded local commits.
    func fetchWorkoutEventPage(workoutID: UUID, offset: Int = 0, limit: Int = 100) async throws -> WorkoutEventPage {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        guard offset >= 0, (1...100).contains(limit) else { throw HealthCollectionError.invalidDateRange }
        guard let workout = try await workout(with: workoutID) else {
            throw HealthCollectionError.unsupportedType("workout \(workoutID.uuidString) unavailable")
        }
        let all = workout.workoutEvents ?? []
        let upper = min(all.count, offset + limit)
        guard offset < upper else { return WorkoutEventPage(events: [], nextOffset: nil) }
        let source = workout.sourceRevision.source
        let events = (offset..<upper).map { index in
            let entry = all[index]
            return HealthEvent(eventID: "workout:\(workoutID.uuidString):event:\(index)",
                               revision: 1, operation: "upsert", kind: "category",
                               type: "boaz.workout.event", sourceBundleID: source.bundleIdentifier,
                               sourceName: source.name, startUTC: entry.dateInterval.start,
                               endUTC: entry.dateInterval.end, value: Double(entry.type.rawValue),
                               unit: "workout-event-type",
                               metadata: ["workout_id": workoutID.uuidString, "event_index": String(index)])
        }
        return WorkoutEventPage(events: events, nextOffset: upper < all.count ? upper : nil)
    }

    /// Heart-rate samples attached by HealthKit to a workout. This has an independent
    /// anchor per workout and must be drained and committed just like a normal type.
    func collectWorkoutHeartRatePage(workoutID: UUID, anchorData: Data?) async throws -> HealthImportPage {
        guard isAvailable else { throw HealthCollectionError.unavailable }
        guard let heartRateType = HKObjectType.quantityType(forIdentifier: .heartRate) else {
            throw HealthCollectionError.unsupportedType("heartRate")
        }
        guard let workout = try await workout(with: workoutID) else {
            throw HealthCollectionError.unsupportedType("workout \(workoutID.uuidString) unavailable")
        }
        let result = try await anchoredPage(
            sampleType: heartRateType,
            predicate: HKQuery.predicateForObjects(from: workout),
            anchor: try decodeAnchor(anchorData)
        )
        let prefix = "workout:\(workoutID.uuidString):heart-rate:"
        let converted = try result.samples.map { sample -> HealthEvent in
            guard let quantity = sample as? HKQuantitySample,
                  var base = event(from: quantity) else {
                throw HealthCollectionError.unconvertibleSample(sample.uuid.uuidString)
            }
            var metadata = base.metadata
            metadata["workout_id"] = workoutID.uuidString
            metadata["healthkit_sample_id"] = quantity.uuid.uuidString
            base = HealthEvent(eventID: prefix + quantity.uuid.uuidString, revision: 1,
                               operation: "upsert", kind: "quantity", type: "boaz.workout.heart_rate",
                               sourceBundleID: base.sourceBundleID, sourceName: base.sourceName,
                               startUTC: base.startUTC, endUTC: base.endUTC,
                               value: base.value, unit: base.unit, metadata: metadata)
            return base
        }
        let events = converted + result.deleted.map {
            HealthEvent(eventID: prefix + $0.uuid.uuidString, revision: 2,
                        operation: "delete", kind: "quantity", type: "boaz.workout.heart_rate",
                        sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                        value: nil, unit: nil, metadata: ["workout_id": workoutID.uuidString])
        }
        return HealthImportPage(typeIdentifier: "workout-heart-rate:\(workoutID.uuidString)",
                                events: events, nextAnchorData: try encodeAnchor(result.anchor),
                                countReachedLimit: result.samples.count + result.deleted.count >= HealthTypeCatalog.pageLimit)
    }

    /// HealthKit's completion is called only after the caller has persisted a collection
    /// pass, even when that pass fails. The caller reports failures through `onError`.
    /// Background delivery may be deferred or coalesced by iOS.
    func startObserving(
        onChange: @escaping @Sendable (String) async throws -> Void,
        onError: @escaping @Sendable (String, String) -> Void
    ) async -> BackgroundDeliveryReport {
        guard isAvailable else {
            return BackgroundDeliveryReport(enabledTypes: [], failures: ["HealthKit": "Health data unavailable"])
        }
        let installObservers = observerQueries.isEmpty
        var enabled: [String] = []
        var failures: [String: String] = [:]
        for sampleType in HealthTypeCatalog.sampleTypes {
            let identifier = sampleType.identifier
            if installObservers {
                let query = HKObserverQuery(sampleType: sampleType, predicate: nil) { _, completion, error in
                    if let error {
                        onError(identifier, error.localizedDescription)
                        completion()
                        return
                    }
                    let completionToken = ObserverCompletion(body: completion)
                    Task {
                        defer { completionToken.finish() }
                        do { try await onChange(identifier) }
                        catch { onError(identifier, error.localizedDescription) }
                    }
                }
                observerQueries.append(query)
                store.execute(query)
            }
            let failure: String? = await withCheckedContinuation { continuation in
                store.enableBackgroundDelivery(for: sampleType, frequency: .hourly) { success, error in
                    continuation.resume(returning: success ? nil : (error?.localizedDescription ?? "Background delivery unavailable"))
                }
            }
            if let failure { failures[identifier] = failure }
            else { enabled.append(identifier) }
        }
        return BackgroundDeliveryReport(enabledTypes: enabled, failures: failures)
    }

    func stopObserving() {
        for query in observerQueries { store.stop(query) }
        observerQueries.removeAll()
    }

    private struct AnchoredResult {
        let samples: [HKSample]
        let deleted: [HKDeletedObject]
        let anchor: HKQueryAnchor
    }

    private func anchoredPage(sampleType: HKSampleType, predicate: NSPredicate?, anchor: HKQueryAnchor?) async throws -> AnchoredResult {
        try await withCheckedThrowingContinuation { continuation in
            let query = HKAnchoredObjectQuery(type: sampleType, predicate: predicate, anchor: anchor,
                                              limit: HealthTypeCatalog.pageLimit) { _, samples, deleted, newAnchor, error in
                if let error { continuation.resume(throwing: error); return }
                guard let newAnchor else {
                    continuation.resume(throwing: HealthCollectionError.noAnchorReturned)
                    return
                }
                continuation.resume(returning: AnchoredResult(samples: samples ?? [], deleted: deleted ?? [], anchor: newAnchor))
            }
            store.execute(query)
        }
    }

    private func workout(with id: UUID) async throws -> HKWorkout? {
        try await withCheckedThrowingContinuation { continuation in
            let query = HKSampleQuery(sampleType: HKObjectType.workoutType(),
                                      predicate: HKQuery.predicateForObject(with: id), limit: 1,
                                      sortDescriptors: nil) { _, samples, error in
                if let error { continuation.resume(throwing: error) }
                else { continuation.resume(returning: samples?.first as? HKWorkout) }
            }
            store.execute(query)
        }
    }

    private func decodeAnchor(_ data: Data?) throws -> HKQueryAnchor? {
        guard let data else { return nil }
        do {
            guard let anchor = try NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data) else {
                throw HealthCollectionError.invalidAnchor
            }
            return anchor
        }
        catch { throw HealthCollectionError.invalidAnchor }
    }

    private func encodeAnchor(_ anchor: HKQueryAnchor) throws -> Data {
        try NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true)
    }

    private func kind(for type: HKSampleType) -> String {
        if type is HKQuantityType { return "quantity" }
        if type is HKCategoryType { return "category" }
        return "workout"
    }

    private func event(from sample: HKSample) -> HealthEvent? {
        let source = sample.sourceRevision.source
        let sourceID = source.bundleIdentifier
        let sourceName = source.name.isEmpty ? sourceID : source.name
        var metadata: [String: String] = [:]
        if let version = sample.sourceRevision.version { metadata["source_version"] = String(version.prefix(64)) }
        if let model = sample.device?.model { metadata["device_model"] = String(model.prefix(64)) }
        if let userEntered = sample.metadata?[HKMetadataKeyWasUserEntered] as? Bool {
            metadata["user_entered"] = userEntered ? "true" : "false"
        }

        if let quantity = sample as? HKQuantitySample {
            guard let canonical = HealthTypeCatalog.canonicalUnit(for: sample.sampleType.identifier) else { return nil }
            let value = quantity.quantity.doubleValue(for: canonical.unit) * canonical.multiplier
            guard value.isFinite else { return nil }
            // HealthKit does not expose the source app's entered unit. Preserve a bounded
            // quantity representation for audit, while the numeric value uses a fixed unit.
            metadata["healthkit_quantity_repr"] = String(quantity.quantity.description.prefix(128))
            return HealthEvent(eventID: sample.uuid.uuidString, revision: 1, operation: "upsert",
                               kind: "quantity", type: sample.sampleType.identifier,
                               sourceBundleID: sourceID, sourceName: sourceName,
                               startUTC: sample.startDate, endUTC: sample.endDate,
                               value: value, unit: canonical.label, metadata: metadata)
        }
        if let category = sample as? HKCategorySample {
            return HealthEvent(eventID: sample.uuid.uuidString, revision: 1, operation: "upsert",
                               kind: "category", type: sample.sampleType.identifier,
                               sourceBundleID: sourceID, sourceName: sourceName,
                               startUTC: sample.startDate, endUTC: sample.endDate,
                               value: Double(category.value), unit: "category-value", metadata: metadata)
        }
        if let workout = sample as? HKWorkout {
            metadata["activity_type"] = String(workout.workoutActivityType.rawValue)
            if let type = HKObjectType.quantityType(forIdentifier: .activeEnergyBurned),
               let energy = workout.statistics(for: type)?.sumQuantity() {
                metadata["total_energy_kcal"] = String(energy.doubleValue(for: .kilocalorie()))
            }
            for identifier: HKQuantityTypeIdentifier in [.distanceWalkingRunning, .distanceCycling, .distanceSwimming] {
                if let type = HKObjectType.quantityType(forIdentifier: identifier),
                   let distance = workout.statistics(for: type)?.sumQuantity() {
                    metadata["total_distance_m"] = String(distance.doubleValue(for: .meter()))
                    break
                }
            }
            metadata["workout_event_count"] = String(workout.workoutEvents?.count ?? 0)
            return HealthEvent(eventID: sample.uuid.uuidString, revision: 1, operation: "upsert",
                               kind: "workout", type: sample.sampleType.identifier,
                               sourceBundleID: sourceID, sourceName: sourceName,
                               startUTC: sample.startDate, endUTC: sample.endDate,
                               value: workout.duration, unit: "s", metadata: metadata)
        }
        return nil
    }

    private func activityEvents(from summary: HKActivitySummary) throws -> [HealthEvent] {
        let calendar = Calendar.current
        let components = summary.dateComponents(for: calendar)
        guard let dayStart = calendar.date(from: components),
              let dayEnd = calendar.date(byAdding: .day, value: 1, to: dayStart) else {
            throw HealthCollectionError.invalidDateRange
        }
        let day = String(format: "%04d-%02d-%02d", components.year ?? 0, components.month ?? 0, components.day ?? 0)
        let zone = calendar.timeZone.identifier
        let measures: [(String, Double, String, Double)] = [
            ("move", summary.activeEnergyBurned.doubleValue(for: .kilocalorie()), "kcal",
             summary.activeEnergyBurnedGoal.doubleValue(for: .kilocalorie())),
            ("exercise", summary.appleExerciseTime.doubleValue(for: .minute()), "min",
             summary.appleExerciseTimeGoal.doubleValue(for: .minute())),
            ("stand", summary.appleStandHours.doubleValue(for: .count()), "hours",
             summary.appleStandHoursGoal.doubleValue(for: .count()))
        ]
        return try measures.map { name, value, unit, goal in
            guard value.isFinite, goal.isFinite else {
                throw HealthCollectionError.unconvertibleSample("activity:\(day):\(name)")
            }
            return HealthEvent(eventID: "activity:\(day):\(name)", revision: 0, operation: "upsert",
                               kind: "activity", type: "activity.\(name)",
                               sourceBundleID: "healthkit.activity_summary", sourceName: "HealthKit Activity Summary",
                               startUTC: dayStart, endUTC: dayEnd, value: value, unit: unit,
                               metadata: ["day": day, "time_zone": zone, "goal": String(goal),
                                          "source_kind": "merged_aggregate"])
        }
    }
}
