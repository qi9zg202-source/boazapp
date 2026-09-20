import Foundation
import HealthKit

@MainActor
final class BoazCoordinator {
    static let shared = BoazCoordinator()

    let model = HealthDashboardModel()
    private let health = HealthKitManager()
    private let gateway = TokyoCloudGateway()
    private let database: BoazLocalDatabase?
    private let engine: SyncEngine?
    private var running = false
    private var rerunRequested = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private let healthRequestKey = "boaz.health.readRequestProcessed"

    private init() {
        do {
            let database = try BoazLocalDatabase()
            self.database = database
            engine = SyncEngine(health: health, database: database, gateway: gateway)
        } catch {
            database = nil
            engine = nil
            model.errorMessage = error.localizedDescription
            model.cloudState = .failure("The protected local database could not open.")
        }
        model.uploadConsentGranted = BoazConfiguration.uploadConsent
        model.isPaired = (try? TokyoCredentialStore.token()) != nil
        model.onSync = { [weak self] in await self?.manualSync() }
        model.onRequestHealthAccess = { [weak self] in await self?.requestHealthAccess() }
        model.onPair = { [weak self] endpoint, code in await self?.pair(endpoint: endpoint, code: code) }
        model.onSetUploadConsent = { [weak self] enabled in await self?.setUploadConsent(enabled) }
        model.onStopUpload = { [weak self] in await self?.setUploadConsent(false) }
        model.onEraseCloud = { [weak self] in await self?.eraseCloudCopy() }
        model.onRefresh = { [weak self] in await self?.refresh() }
    }

    func startBackgroundDelivery() async {
        guard database != nil, UserDefaults.standard.bool(forKey: healthRequestKey) else { return }
        let report = await health.startObserving(
            onChange: { [weak self] _ in
                guard let self else { return }
                await self.backgroundSync()
            },
            onError: { [weak self] type, message in
                Task { @MainActor in
                    guard let self, let database = self.database else { return }
                    try? await database.recordFailure(phase: "background", detail: "\(type): \(message)")
                    await self.refresh()
                }
            }
        )
        if !report.failures.isEmpty {
            model.errorMessage = "Background Health updates are unavailable for some data types. Manual sync remains available."
        }
        await refresh()
    }

    private func requestHealthAccess() async {
        do {
            try await health.requestReadAccess()
            UserDefaults.standard.set(true, forKey: healthRequestKey)
            await startBackgroundDelivery()
            await refresh()
        } catch {
            model.errorMessage = error.localizedDescription
        }
    }

    private func manualSync() async {
        if !UserDefaults.standard.bool(forKey: healthRequestKey) {
            await requestHealthAccess()
            guard UserDefaults.standard.bool(forKey: healthRequestKey) else { return }
        }
        await runSync()
    }

    private func backgroundSync() async {
        guard UserDefaults.standard.bool(forKey: healthRequestKey) else { return }
        await runSync()
    }

    private func runSync() async {
        guard let engine else { return }
        if running {
            rerunRequested = true
            await withCheckedContinuation { continuation in waiters.append(continuation) }
            return
        }
        running = true
        model.isWorking = true
        repeat {
            rerunRequested = false
            model.syncPhase = .collecting
            let result = await engine.collect()
            var uploadFailed = false
            await refresh()
            if result.savedEvents > 0 { model.cloudState = .localSaved }
            if !result.failedSources.isEmpty {
                model.errorMessage = "Some Health data could not be read or saved. Open Sync audit for details."
            }
            if BoazConfiguration.uploadConsent {
                if (try? TokyoCredentialStore.token()) != nil {
                    model.syncPhase = .committing
                    model.cloudState = .uploading
                    let upload = await engine.uploadPending()
                    if let receipt = upload.lastReceipt {
                        model.snapshot.lastBatchSampleCount = receipt.acceptedEvents
                        model.snapshot.lastCloudReceiptAt = ISO8601DateFormatter().date(from: receipt.receivedAt)
                        if receipt.status == "metrics_current" {
                            model.cloudState = .metricsCurrent
                            model.snapshot.lastProjectedAt = receipt.projectedAt.flatMap { ISO8601DateFormatter().date(from: $0) }
                        } else {
                            model.cloudState = .metricsPending
                        }
                    }
                    if let failure = upload.failure {
                        uploadFailed = true
                        model.cloudState = .offline
                        model.errorMessage = "Upload is queued on this iPhone: \(failure)"
                    }
                } else {
                    model.cloudState = .pairingRequired
                }
            } else {
                model.cloudState = .localOnly
            }
            await refresh()
            model.syncPhase = result.failedSources.isEmpty && !uploadFailed ? .finished : .failed
        } while rerunRequested && !Task.isCancelled
        model.isWorking = false
        running = false
        let completed = waiters
        waiters.removeAll()
        completed.forEach { $0.resume() }
    }

    private func pair(endpoint: URL, code: String) async {
        do {
            guard try TokyoErasureStore.current() == nil else {
                model.errorMessage = "Wait for the previous cloud erasure to finish before pairing again."
                return
            }
            try await gateway.pair(endpoint: endpoint, code: code, deviceID: BoazConfiguration.deviceID)
            model.isPaired = true
            model.cloudState = BoazConfiguration.uploadConsent ? .queued : .localOnly
            await refresh()
        } catch {
            model.errorMessage = error.localizedDescription
            model.cloudState = .failure("Private Tokyo pairing failed.")
        }
    }

    private func setUploadConsent(_ enabled: Bool) async {
        if enabled, (try? TokyoErasureStore.current()) != nil {
            model.errorMessage = "Wait for cloud erasure confirmation before enabling upload again."
            return
        }
        BoazConfiguration.uploadConsent = enabled
        model.uploadConsentGranted = enabled
        model.cloudState = enabled ? (model.isPaired ? .queued : .pairingRequired) : .localOnly
        if enabled && model.isPaired { await runSync() }
        else { await refresh() }
    }

    private func eraseCloudCopy() async {
        BoazConfiguration.uploadConsent = false
        model.uploadConsentGranted = false
        model.cloudState = .erasurePending
        do {
            let response = try await gateway.eraseCloudCopy()
            try await database?.requeueAfterErasure(id: response.erasureID)
            try await database?.recordErasureStatus(id: response.erasureID, status: response.status)
            if response.status == "complete" && database != nil { try TokyoErasureStore.delete() }
            model.isPaired = false
            model.cloudState = response.status == "complete" ? .activeErasureConfirmed : .erasurePending
            await refresh()
        } catch {
            model.errorMessage = "Cloud erasure has not been confirmed. Retry using the same recovery request: \(error.localizedDescription)"
            model.cloudState = .erasurePending
        }
    }

    func refresh() async {
        guard let database else { return }
        do {
            if let erasure = try await gateway.erasureStatus() {
                try await database.requeueAfterErasure(id: erasure.erasureID)
                try await database.recordErasureStatus(id: erasure.erasureID, status: erasure.status)
                if erasure.status == "complete" { try TokyoErasureStore.delete() }
                model.isPaired = false
                model.cloudState = erasure.status == "complete" ? .activeErasureConfirmed : .erasurePending
            } else if !model.isPaired && !BoazConfiguration.uploadConsent {
                if try await database.lastErasureCompleted() {
                    model.cloudState = .activeErasureConfirmed
                }
            }
        } catch {
            // An offline private link does not change the last confirmed erasure state.
        }
        do {
            let counts = try await database.counts()
            let audit = try await database.audit()
            var snapshot = model.snapshot
            snapshot.localSampleCount = counts.records
            snapshot.pendingSampleCount = counts.pending
            snapshot.lastImportedAt = audit.first(where: { $0.phase == "collect" && $0.outcome == "local_saved" })?.createdAt
            snapshot.lastCloudReceiptAt = audit.first(where: { $0.phase == "upload" && $0.outcome == "cloud_saved" })?.createdAt
            snapshot.lastProjectedAt = audit.first(where: { $0.phase == "project" && $0.outcome == "metrics_current" })?.createdAt
            snapshot.lastBatchSampleCount = audit.first(where: { $0.phase == "upload" && $0.outcome == "cloud_saved" })?.eventCount
            snapshot.audit = audit.map { entry in
                SyncAuditEntry(id: String(entry.id), occurredAt: entry.createdAt,
                               title: "\(entry.phase.capitalized) · \(entry.outcome.replacingOccurrences(of: "_", with: " "))",
                               detail: entry.detail,
                               severity: entry.outcome == "failed" ? .failure : (entry.outcome == "retry_queued" ? .warning : .information))
            }
            snapshot.sleep = try await sleepDisplay(database: database)
            snapshot.fitness = try await fitnessDisplay(database: database)
            snapshot.vitals = try await vitalsDisplay(database: database)
            model.snapshot = snapshot
            let preserveErasure = model.cloudState == .activeErasureConfirmed && !model.isPaired && !BoazConfiguration.uploadConsent
            if !model.isWorking && model.cloudState != .erasurePending && !preserveErasure {
                if !BoazConfiguration.uploadConsent { model.cloudState = .localOnly }
                else if !model.isPaired { model.cloudState = .pairingRequired }
                else if counts.pending > 0 { model.cloudState = .queued }
                else if counts.cloudSaved > 0 { model.cloudState = .metricsPending }
                else if counts.metricsCurrent > 0 { model.cloudState = .metricsCurrent }
                else { model.cloudState = .localSaved }
            }
        } catch {
            model.errorMessage = error.localizedDescription
            model.cloudState = .failure("Local health records could not be read.")
        }
    }

    private func sleepDisplay(database: BoazLocalDatabase) async throws -> SleepDisplay? {
        let since = Date().addingTimeInterval(-7 * 24 * 3600)
        let events = try await database.events(typeIdentifier: HKCategoryTypeIdentifier.sleepAnalysis.rawValue, since: since)
        let sessions = SleepAnalyzer.sessions(from: SleepAnalyzer.segments(from: events))
        guard let session = SleepAnalyzer.latestCompletedSession(from: sessions) else { return nil }
        func average(_ type: String) async throws -> Double? {
            let values = try await database.events(typeIdentifier: type, since: session.start).filter {
                guard let start = $0.startUTC else { return false }
                return start <= session.end && $0.value?.isFinite == true
            }.compactMap(\.value)
            guard !values.isEmpty else { return nil }
            return values.reduce(0, +) / Double(values.count)
        }
        return SleepDisplay(
            startedAt: session.start, endedAt: session.end, totalMinutes: session.asleepSeconds / 60,
            deepMinutes: session.stages.deepSeconds / 60, coreMinutes: session.stages.coreSeconds / 60,
            remMinutes: session.stages.remSeconds / 60, awakeMinutes: session.stages.awakeSeconds / 60,
            unspecifiedMinutes: session.stages.unspecifiedSeconds / 60,
            inBedMinutes: session.inBedSeconds > 0 ? session.inBedSeconds / 60 : nil,
            sourceCount: session.sampleCount,
            wristTemperatureC: try await average(HKQuantityTypeIdentifier.appleSleepingWristTemperature.rawValue),
            overnightHeartRate: try await average(HKQuantityTypeIdentifier.heartRate.rawValue),
            overnightRespiratoryRate: try await average(HKQuantityTypeIdentifier.respiratoryRate.rawValue),
            overnightOxygenPercent: try await average(HKQuantityTypeIdentifier.oxygenSaturation.rawValue),
            overnightHRVMilliseconds: try await average(HKQuantityTypeIdentifier.heartRateVariabilitySDNN.rawValue)
        )
    }

    private func fitnessDisplay(database: BoazLocalDatabase) async throws -> FitnessDisplay? {
        let today = Calendar.current.startOfDay(for: Date())
        let move = try await database.events(typeIdentifier: "activity.move", since: today, limit: 1).first
        let exercise = try await database.events(typeIdentifier: "activity.exercise", since: today, limit: 1).first
        let stand = try await database.events(typeIdentifier: "activity.stand", since: today, limit: 1).first
        let workouts = try await database.events(typeIdentifier: HKObjectType.workoutType().identifier, since: Date().addingTimeInterval(-30 * 24 * 3600), limit: 20)
        let workoutRates = try await database.events(typeIdentifier: "boaz.workout.heart_rate", since: Date().addingTimeInterval(-30 * 24 * 3600), limit: 100_000)
        let ratesByWorkout = Dictionary(grouping: workoutRates) { $0.metadata["workout_id"] ?? "" }
        let cumulative = try? await health.fetchTodayCumulative(now: Date())
        let recent = workouts.compactMap { event -> WorkoutDisplay? in
            guard let started = event.startUTC, let duration = event.value else { return nil }
            let activity = event.metadata["activity_type"]
            let title: String
            switch activity {
            case String(HKWorkoutActivityType.swimming.rawValue): title = "Swimming"
            case String(HKWorkoutActivityType.running.rawValue): title = "Running"
            case String(HKWorkoutActivityType.cycling.rawValue): title = "Cycling"
            case String(HKWorkoutActivityType.functionalStrengthTraining.rawValue): title = "Strength training"
            default: title = "Workout"
            }
            let readings = (ratesByWorkout[event.eventID] ?? []).compactMap(\.value).filter(\.isFinite)
            let heartRate = readings.isEmpty ? nil : MetricDisplay(
                value: readings.reduce(0, +) / Double(readings.count), unit: "bpm",
                sampleCount: readings.count, detail: "\(Int(readings.min() ?? 0))–\(Int(readings.max() ?? 0)) bpm"
            )
            return WorkoutDisplay(id: event.eventID, title: title, startedAt: started,
                                  durationMinutes: duration / 60,
                                  energyKcal: event.metadata["total_energy_kcal"].flatMap(Double.init),
                                  distance: event.metadata["total_distance_m"].flatMap(Double.init).map { MetricDisplay(value: $0, unit: "m") },
                                  heartRate: heartRate,
                                  eventCount: Int(event.metadata["workout_event_count"] ?? "0") ?? 0)
        }
        guard move != nil || exercise != nil || stand != nil || !recent.isEmpty || cumulative != nil else { return nil }
        return FitnessDisplay(day: today,
                              moveKcal: move?.value, moveGoalKcal: move?.metadata["goal"].flatMap(Double.init),
                              exerciseMinutes: exercise?.value, exerciseGoalMinutes: exercise?.metadata["goal"].flatMap(Double.init),
                              standHours: stand?.value, standGoalHours: stand?.metadata["goal"].flatMap(Double.init),
                              steps: cumulative?.steps, flights: cumulative?.flights, recentWorkouts: recent)
    }

    private func vitalsDisplay(database: BoazLocalDatabase) async throws -> VitalsDisplay? {
        let types: [(String, String)] = [
            ("heartRate", HKQuantityTypeIdentifier.heartRate.rawValue),
            ("restingHeartRate", HKQuantityTypeIdentifier.restingHeartRate.rawValue),
            ("oxygenPercent", HKQuantityTypeIdentifier.oxygenSaturation.rawValue),
            ("weightKg", HKQuantityTypeIdentifier.bodyMass.rawValue),
            ("hrvMilliseconds", HKQuantityTypeIdentifier.heartRateVariabilitySDNN.rawValue),
            ("systolicMMHg", HKQuantityTypeIdentifier.bloodPressureSystolic.rawValue),
            ("diastolicMMHg", HKQuantityTypeIdentifier.bloodPressureDiastolic.rawValue),
            ("bodyFatPercent", HKQuantityTypeIdentifier.bodyFatPercentage.rawValue),
            ("bmi", HKQuantityTypeIdentifier.bodyMassIndex.rawValue)
        ]
        var values: [String: HealthEvent] = [:]
        for (key, type) in types { values[key] = try await database.latestEvent(typeIdentifier: type) }
        guard !values.isEmpty else { return nil }
        return VitalsDisplay(
            heartRate: values["heartRate"]?.value, restingHeartRate: values["restingHeartRate"]?.value,
            oxygenPercent: values["oxygenPercent"]?.value, weightKg: values["weightKg"]?.value,
            hrvMilliseconds: values["hrvMilliseconds"]?.value, systolicMMHg: values["systolicMMHg"]?.value,
            diastolicMMHg: values["diastolicMMHg"]?.value, bodyFatPercent: values["bodyFatPercent"]?.value,
            bmi: values["bmi"]?.value,
            timestamps: values.compactMapValues(\.startUTC),
            sampleCounts: values.mapValues { _ in 1 }
        )
    }
}
