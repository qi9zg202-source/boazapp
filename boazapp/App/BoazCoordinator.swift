import Foundation
import HealthKit

enum SyncRunPresentation {
    static func phase(
        collection: CollectionReport, uploadFailed: Bool, uploadCancelled: Bool, taskCancelled: Bool
    ) -> SyncPhase {
        if collection.storageProtectionUnverified || uploadFailed { return .failed }
        if collection.cancelled || uploadCancelled || taskCancelled { return .cancelled }
        return collection.failedSources.isEmpty ? .finished : .failed
    }

    static func cancelledUploadState(
        counts: LocalHealthCounts?, lastReceipt: TokyoReceipt?, snapshot: DashboardSnapshot
    ) -> CloudDisplayState {
        if let counts {
            if counts.pending > 0 { return .queued }
            if counts.cloudSaved > 0 { return .metricsPending }
            if counts.metricsCurrent > 0 { return .metricsCurrent }
            return counts.records > 0 ? .localSaved : .localOnly
        }
        if snapshot.pendingSampleCount > 0 { return .queued }
        if let lastReceipt {
            return lastReceipt.status == "metrics_current" ? .metricsCurrent : .metricsPending
        }
        return snapshot.localSampleCount > 0 ? .localSaved : .localOnly
    }
}

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
    private var waiters: [UUID: CheckedContinuation<Void, Never>] = [:]
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
        let erasurePending: Bool
        do {
            erasurePending = try TokyoErasureStore.current() != nil
        } catch {
            erasurePending = true
            model.errorMessage = "Cloud erasure recovery is unavailable; pairing and upload remain blocked."
        }
        model.isPaired = (try? TokyoCredentialStore.token()) != nil && !erasurePending
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
            let waiterID = UUID()
            await withTaskCancellationHandler {
                await withCheckedContinuation { continuation in
                    if Task.isCancelled {
                        continuation.resume()
                    } else {
                        waiters[waiterID] = continuation
                        rerunRequested = true
                    }
                }
            } onCancel: {
                Task { @MainActor [weak self] in self?.cancelRerunWaiter(waiterID) }
            }
            return
        }
        running = true
        model.isWorking = true
        repeat {
            rerunRequested = false
            model.syncPhase = .collecting
            let result = await engine.collect()
            var uploadFailed = false
            var uploadCancelled = false
            var cancelledCloudState: CloudDisplayState?
            await refresh()
            if result.savedEvents > 0 { model.cloudState = .localSaved }
            if result.storageProtectionUnverified {
                model.cloudState = .failure("Local file protection is unverified; upload is blocked.")
                model.errorMessage = "Some records may already be saved on this iPhone, but their file protection could not be verified. Nothing will upload until it is verified."
            } else if !result.failedSources.isEmpty {
                model.errorMessage = "Some Health data could not be read or saved. Open Sync audit for details."
            }
            if result.cancelled || Task.isCancelled {
                model.syncPhase = SyncRunPresentation.phase(
                    collection: result, uploadFailed: false, uploadCancelled: false, taskCancelled: Task.isCancelled
                )
                break
            }
            if result.storageProtectionUnverified {
                uploadFailed = true
            } else if result.allowsUpload && BoazConfiguration.uploadConsent {
                if (try? TokyoCredentialStore.token()) != nil {
                    model.syncPhase = .committing
                    model.cloudState = .uploading
                    let upload = await engine.uploadPending()
                    uploadCancelled = upload.cancelled
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
                        if let database, !(await database.isStorageProtectionVerified()) {
                            model.cloudState = .failure("Local file protection is unverified; upload is blocked.")
                            model.errorMessage = "Upload is blocked until local file protection is verified: \(failure)"
                        } else {
                            model.cloudState = .offline
                            model.errorMessage = "Upload is queued on this iPhone: \(failure)"
                        }
                    }
                    if upload.cancelled {
                        let counts: LocalHealthCounts?
                        if let database { counts = try? await database.counts() }
                        else { counts = nil }
                        cancelledCloudState = SyncRunPresentation.cancelledUploadState(
                            counts: counts, lastReceipt: upload.lastReceipt, snapshot: model.snapshot
                        )
                    }
                } else {
                    model.cloudState = .pairingRequired
                }
            } else {
                model.cloudState = .localOnly
            }
            await refresh()
            if let cancelledCloudState { model.cloudState = cancelledCloudState }
            model.syncPhase = SyncRunPresentation.phase(
                collection: result, uploadFailed: uploadFailed,
                uploadCancelled: uploadCancelled, taskCancelled: Task.isCancelled
            )
        } while rerunRequested && !Task.isCancelled
        model.isWorking = false
        running = false
        rerunRequested = false
        let completed = Array(waiters.values)
        waiters.removeAll()
        completed.forEach { $0.resume() }
    }

    private func cancelRerunWaiter(_ id: UUID) {
        waiters.removeValue(forKey: id)?.resume()
        if waiters.isEmpty { rerunRequested = false }
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
        if enabled {
            do {
                try TokyoErasureStore.requireNoPendingForUpload()
            } catch {
                model.errorMessage = "Cloud erasure recovery is pending or unavailable; upload remains blocked."
                return
            }
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
            guard let database else {
                throw LocalDatabaseError.open("The protected local database is unavailable.")
            }
            let response = try await gateway.eraseCloudCopy()
            let state = try await applyErasureResponse(response, database: database)
            model.cloudState = state == .complete ? .activeErasureConfirmed : .erasurePending
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
                let state = try await applyErasureResponse(erasure, database: database)
                model.cloudState = state == .complete ? .activeErasureConfirmed : .erasurePending
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
                if !(await database.isStorageProtectionVerified()) {
                    model.cloudState = .failure("Local file protection is unverified; upload is blocked.")
                } else if !BoazConfiguration.uploadConsent { model.cloudState = .localOnly }
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

    private func applyErasureResponse(
        _ response: TokyoEraseResponse, database: BoazLocalDatabase
    ) async throws -> TokyoErasureStatus {
        guard let recovery = try TokyoErasureStore.current() else {
            throw TokyoGatewayError.unexpectedResponse
        }
        let state = try response.validate(expectedErasureID: recovery.id)
        try await database.requeueAfterErasure(id: response.erasureID)
        try await database.recordErasureStatus(id: response.erasureID, status: response.status)
        model.isPaired = false

        // Pending receipts keep both recovery material and the old token so a
        // later retry can authenticate through either server-supported path.
        guard state == .complete else { return state }
        _ = try BoazConfiguration.rotateDeviceID(afterCompletedErasure: response.erasureID)
        try TokyoCredentialStore.delete()
        try TokyoErasureStore.delete()
        return state
    }

    private func sleepDisplay(database: BoazLocalDatabase) async throws -> SleepDisplay? {
        guard let session = try await SleepDerivation.latestCompletedSession(database: database) else { return nil }
        func average(_ type: String) async throws -> Double? {
            try await database.averageValue(typeIdentifier: type, from: session.start, through: session.end)
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
        let ratesByWorkout = try await database.workoutHeartRateSummaries(workoutIDs: workouts.map(\.eventID))
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
            let heartRate = ratesByWorkout[event.eventID].map { summary in
                MetricDisplay(value: summary.average, unit: "bpm", sampleCount: summary.sampleCount,
                              detail: "\(Int(summary.minimum))–\(Int(summary.maximum)) bpm")
            }
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
