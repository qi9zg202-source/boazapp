import Foundation

struct CollectionReport: Sendable {
    let savedEvents: Int
    let failedSources: [String]
    let storageProtectionUnverified: Bool
    let cancelled: Bool

    var allowsUpload: Bool { !cancelled && !storageProtectionUnverified }
}

struct UploadReport: Sendable {
    let committedEvents: Int
    let lastReceipt: TokyoReceipt?
    let failure: String?
    let cancelled: Bool
}

/// Coordinates HealthKit page checkpoints, the local outbox, and Tokyo receipts.
/// A callback or a network response alone never advances the authoritative state.
actor SyncEngine {
    private let health: HealthKitManager
    private let database: BoazLocalDatabase
    private let gateway: TokyoCloudGateway
    private let erasureCredential: @Sendable () throws -> TokyoErasureCredential?
    private let activityFullScanKey = "boaz.health.lastFullActivityScan"

    init(health: HealthKitManager, database: BoazLocalDatabase, gateway: TokyoCloudGateway,
         erasureCredential: @escaping @Sendable () throws -> TokyoErasureCredential? = TokyoErasureStore.current) {
        self.health = health
        self.database = database
        self.gateway = gateway
        self.erasureCredential = erasureCredential
    }

    func collect() async -> CollectionReport {
        var saved = 0
        var failed: [String] = []
        for type in await health.typeIdentifiers {
            if Task.isCancelled { break }
            do {
                var anchor = try await database.anchor(for: type)
                var pages = 0
                repeat {
                    try Task.checkCancellation()
                    let page = try await health.collectPage(typeIdentifier: type, anchorData: anchor)
                    saved += try await database.apply(events: page.events, anchor: page.nextAnchorData, typeIdentifier: type)
                    anchor = page.nextAnchorData
                    pages += 1
                    if !page.countReachedLimit { break }
                    if pages % 10 == 0 { await Task.yield() }
                } while !Task.isCancelled
            } catch is CancellationError {
                break
            } catch {
                if Task.isCancelled { break }
                failed.append(type)
                try? await database.recordFailure(phase: "collect", detail: "\(type): \(error.localizedDescription)")
            }
            if Task.isCancelled { break }
        }
        if !Task.isCancelled {
            do { saved += try await collectActivities() }
            catch is CancellationError { }
            catch {
                if Task.isCancelled { return await collectionReport(saved: saved, failed: failed) }
                failed.append("activity summaries")
                try? await database.recordFailure(phase: "collect", detail: "activity summaries: \(error.localizedDescription)")
            }
        }
        if !Task.isCancelled {
            do { saved += try await refreshDerivedSleep() }
            catch is CancellationError { }
            catch {
                if Task.isCancelled { return await collectionReport(saved: saved, failed: failed) }
                failed.append("derived sleep")
                try? await database.recordFailure(phase: "collect", detail: "derived sleep: \(error.localizedDescription)")
            }
        }
        if !Task.isCancelled {
            let details = await collectWorkoutDetails()
            saved += details.savedEvents
            failed += details.failedSources
        }
        return await collectionReport(saved: saved, failed: failed)
    }

    private func collectionReport(saved: Int, failed: [String]) async -> CollectionReport {
        let protectionVerified = await database.isStorageProtectionVerified()
        return CollectionReport(savedEvents: saved, failedSources: failed,
                                storageProtectionUnverified: !protectionVerified, cancelled: Task.isCancelled)
    }

    func uploadPending() async -> UploadReport {
        var committed = 0
        var lastReceipt: TokyoReceipt?
        if Task.isCancelled {
            return UploadReport(committedEvents: 0, lastReceipt: nil, failure: nil, cancelled: true)
        }
        guard BoazConfiguration.uploadConsent else {
            return UploadReport(committedEvents: 0, lastReceipt: nil, failure: nil, cancelled: false)
        }
        do {
            try TokyoErasureStore.requireNoPendingForUpload(read: erasureCredential)
            try await database.verifyStorageProtectionForUpload()
            for id in try await database.cloudSavedBatchIDs() {
                try Task.checkCancellation()
                try TokyoErasureStore.requireNoPendingForUpload(read: erasureCredential)
                if let receipt = try await gateway.receipt(batchID: id), receipt.status == "metrics_current" {
                    try await database.markMetricsCurrent(batchID: id, receipt: receipt)
                    lastReceipt = receipt
                }
            }
            while !Task.isCancelled, let batch = try await database.prepareBatch(deviceID: BoazConfiguration.deviceID) {
                guard BoazConfiguration.uploadConsent else { break }
                do {
                    try await database.verifyStorageProtectionForUpload()
                    try TokyoErasureStore.requireNoPendingForUpload(read: erasureCredential)
                    try Task.checkCancellation()
                    let receipt = try await gateway.upload(batch)
                    guard receipt.status == "cloud_saved" || receipt.status == "metrics_current" else {
                        throw TokyoGatewayError.unexpectedResponse
                    }
                    let receiptData = try JSONEncoder().encode(receipt)
                    try await database.markCloudSaved(batchID: batch.id, receipt: receiptData)
                    if receipt.status == "metrics_current" {
                        try await database.markMetricsCurrent(batchID: batch.id, receipt: receipt)
                    }
                    lastReceipt = receipt
                    committed += receipt.acceptedEvents
                } catch is CancellationError {
                    return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil, cancelled: true)
                } catch {
                    if case LocalDatabaseError.committedButProtectionUnverified = error {
                        // The local transaction already committed; do not recast it as
                        // a failed network request or mutate its retry deadline.
                    } else if case LocalDatabaseError.storageProtectionUnverified = error {
                        // The pre-upload protection recheck failed before network
                        // activity; this is not a batch transport retry.
                    } else if Task.isCancelled || (error as? URLError)?.code == .cancelled {
                        return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil, cancelled: true)
                    } else {
                        try? await database.deferBatch(batchID: batch.id, error: error.localizedDescription)
                    }
                    return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription, cancelled: false)
                }
                await Task.yield()
            }
            return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil, cancelled: Task.isCancelled)
        } catch is CancellationError {
            return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil, cancelled: true)
        } catch {
            if case LocalDatabaseError.committedButProtectionUnverified = error {
                return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription, cancelled: false)
            }
            if case LocalDatabaseError.storageProtectionUnverified = error {
                return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription, cancelled: false)
            }
            if Task.isCancelled || (error as? URLError)?.code == .cancelled {
                return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil, cancelled: true)
            }
            try? await database.recordFailure(phase: "upload", detail: error.localizedDescription)
            return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription, cancelled: false)
        }
    }

    private func collectActivities() async throws -> Int {
        let now = Date()
        let lastFull = UserDefaults.standard.object(forKey: activityFullScanKey) as? Date
        let needsFull = lastFull == nil || now.timeIntervalSince(lastFull!) > 7 * 24 * 3600
        // The history epoch is Gregorian 2014 even if the user's preferred
        // calendar uses another era/year. Keep their local day boundaries.
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = .current
        let start = needsFull ? calendar.date(from: DateComponents(year: 2014, month: 1, day: 1))! : calendar.date(byAdding: .day, value: -7, to: now)!
        var cursor = start
        var saved = 0
        while cursor <= now, !Task.isCancelled {
            let next = min(calendar.date(byAdding: .day, value: 30, to: cursor)!, now)
            let events = try await health.fetchActivitySummaries(from: cursor, through: next)
            saved += try await database.apply(events: events)
            if next == now { break }
            cursor = calendar.date(byAdding: .day, value: 1, to: calendar.startOfDay(for: next))!
        }
        if needsFull && !Task.isCancelled { UserDefaults.standard.set(now, forKey: activityFullScanKey) }
        return saved
    }

    private func refreshDerivedSleep() async throws -> Int {
        try await SleepDerivation.refresh(database: database)
    }

    private func collectWorkoutDetails() async -> WorkoutDetailPassReport {
        await WorkoutDetailDispatcher.run(database: database) { id in
            await self.collectWorkoutDetails(id: id)
        }
    }

    private func collectWorkoutDetails(id: UUID) async -> WorkoutDetailResult {
        var saved = 0
        do {
            guard var progress = try await database.workoutProgress(id: id) else {
                return WorkoutDetailResult(savedEvents: 0, failure: nil)
            }
            if !progress.eventsDone {
                repeat {
                    try Task.checkCancellation()
                    let page = try await health.fetchWorkoutEventPage(workoutID: id, offset: progress.offset, limit: 100)
                    saved += try await database.apply(events: page.events)
                    let newOffset = page.nextOffset ?? progress.offset + page.events.count
                    try await database.advanceWorkoutEvents(id: id, nextOffset: newOffset, done: page.nextOffset == nil)
                    progress.offset = newOffset
                    if page.nextOffset == nil { break }
                } while !Task.isCancelled
            }
            if !progress.heartRateDone && !Task.isCancelled {
                let anchorKey = "workout-heart-rate:\(id.uuidString)"
                var anchor = try await database.anchor(for: anchorKey)
                repeat {
                    try Task.checkCancellation()
                    let page = try await health.collectWorkoutHeartRatePage(workoutID: id, anchorData: anchor)
                    saved += try await database.apply(events: page.events, anchor: page.nextAnchorData, typeIdentifier: anchorKey)
                    anchor = page.nextAnchorData
                    if !page.countReachedLimit { break }
                } while !Task.isCancelled
                if !Task.isCancelled { try await database.markWorkoutHeartRateDone(id: id) }
            }
            return WorkoutDetailResult(savedEvents: saved, failure: nil)
        } catch is CancellationError {
            return WorkoutDetailResult(savedEvents: saved, failure: nil)
        } catch {
            return WorkoutDetailResult(savedEvents: saved, failure: error.localizedDescription)
        }
    }
}
