import Foundation

struct CollectionReport: Sendable {
    let savedEvents: Int
    let failedSources: [String]
}

struct UploadReport: Sendable {
    let committedEvents: Int
    let lastReceipt: TokyoReceipt?
    let failure: String?
}

/// Coordinates HealthKit page checkpoints, the local outbox, and Tokyo receipts.
/// A callback or a network response alone never advances the authoritative state.
actor SyncEngine {
    private let health: HealthKitManager
    private let database: BoazLocalDatabase
    private let gateway: TokyoCloudGateway
    private let activityFullScanKey = "boaz.health.lastFullActivityScan"

    init(health: HealthKitManager, database: BoazLocalDatabase, gateway: TokyoCloudGateway) {
        self.health = health
        self.database = database
        self.gateway = gateway
    }

    func collect() async -> CollectionReport {
        var saved = 0
        var failed: [String] = []
        for type in await health.typeIdentifiers {
            do {
                var anchor = try await database.anchor(for: type)
                var pages = 0
                repeat {
                    let page = try await health.collectPage(typeIdentifier: type, anchorData: anchor)
                    saved += try await database.apply(events: page.events, anchor: page.nextAnchorData, typeIdentifier: type)
                    anchor = page.nextAnchorData
                    pages += 1
                    if !page.countReachedLimit { break }
                    if pages % 10 == 0 { await Task.yield() }
                } while !Task.isCancelled
            } catch {
                failed.append(type)
                try? await database.recordFailure(phase: "collect", detail: "\(type): \(error.localizedDescription)")
            }
            if Task.isCancelled { break }
        }
        if !Task.isCancelled {
            do { saved += try await collectActivities() }
            catch {
                failed.append("activity summaries")
                try? await database.recordFailure(phase: "collect", detail: "activity summaries: \(error.localizedDescription)")
            }
        }
        if !Task.isCancelled {
            do { saved += try await refreshDerivedSleep() }
            catch {
                failed.append("derived sleep")
                try? await database.recordFailure(phase: "collect", detail: "derived sleep: \(error.localizedDescription)")
            }
        }
        if !Task.isCancelled {
            do { saved += try await collectWorkoutDetails() }
            catch {
                failed.append("workout details")
                try? await database.recordFailure(phase: "collect", detail: "workout details: \(error.localizedDescription)")
            }
        }
        return CollectionReport(savedEvents: saved, failedSources: failed)
    }

    func uploadPending() async -> UploadReport {
        var committed = 0
        var lastReceipt: TokyoReceipt?
        guard BoazConfiguration.uploadConsent else {
            return UploadReport(committedEvents: 0, lastReceipt: nil, failure: nil)
        }
        do {
            for id in try await database.cloudSavedBatchIDs() {
                if let receipt = try await gateway.receipt(batchID: id), receipt.status == "metrics_current" {
                    try await database.markMetricsCurrent(batchID: id)
                    lastReceipt = receipt
                }
            }
            while !Task.isCancelled, let batch = try await database.prepareBatch(deviceID: BoazConfiguration.deviceID) {
                guard BoazConfiguration.uploadConsent else { break }
                do {
                    let receipt = try await gateway.upload(batch)
                    guard receipt.status == "cloud_saved" || receipt.status == "metrics_current" else {
                        throw TokyoGatewayError.unexpectedResponse
                    }
                    let receiptData = try JSONEncoder().encode(receipt)
                    try await database.markCloudSaved(batchID: batch.id, receipt: receiptData)
                    if receipt.status == "metrics_current" {
                        try await database.markMetricsCurrent(batchID: batch.id)
                    }
                    lastReceipt = receipt
                    committed += receipt.acceptedEvents
                } catch {
                    try? await database.deferBatch(batchID: batch.id, error: error.localizedDescription)
                    return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription)
                }
                await Task.yield()
            }
            return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: nil)
        } catch {
            try? await database.recordFailure(phase: "upload", detail: error.localizedDescription)
            return UploadReport(committedEvents: committed, lastReceipt: lastReceipt, failure: error.localizedDescription)
        }
    }

    private func collectActivities() async throws -> Int {
        let now = Date()
        let lastFull = UserDefaults.standard.object(forKey: activityFullScanKey) as? Date
        let needsFull = lastFull == nil || now.timeIntervalSince(lastFull!) > 7 * 24 * 3600
        let calendar = Calendar.current
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
        let sleepType = "HKCategoryTypeIdentifierSleepAnalysis"
        let raw = try await database.events(typeIdentifier: sleepType, since: Date(timeIntervalSince1970: 0), limit: 100_000)
        let sessions = SleepAnalyzer.sessions(from: SleepAnalyzer.segments(from: raw))
        let calendar = Calendar.current
        let selected = Dictionary(grouping: sessions.filter { !$0.isNap && $0.asleepSeconds > 0 && $0.end <= Date().addingTimeInterval(-600) }) { session in
            let parts = calendar.dateComponents([.year, .month, .day], from: session.end)
            return String(format: "%04d-%02d-%02d", parts.year ?? 0, parts.month ?? 0, parts.day ?? 0)
        }.compactMapValues { $0.max(by: { $0.asleepSeconds < $1.asleepSeconds }) }
        let currentIDs = Set(selected.keys.map { "sleep-day:\($0)" })
        var derived: [HealthEvent] = selected.map { day, session in
            HealthEvent(
                eventID: "sleep-day:\(day)", revision: 0, operation: "upsert", kind: "quantity",
                type: "boaz.sleep.deep_minutes", sourceBundleID: "boazapp", sourceName: "Boaz SleepAnalyzer",
                startUTC: session.start, endUTC: session.end, value: session.stages.deepSeconds / 60,
                unit: "min", metadata: ["derivation_version": "1", "sleep_day": day, "time_zone": calendar.timeZone.identifier]
            )
        }
        let prior = try await database.events(typeIdentifier: "boaz.sleep.deep_minutes", since: Date(timeIntervalSince1970: 0), limit: 100_000)
        for old in prior where !currentIDs.contains(old.eventID) {
            derived.append(HealthEvent(eventID: old.eventID, revision: 0, operation: "delete", kind: "quantity",
                                       type: old.type, sourceBundleID: "", sourceName: "", startUTC: nil, endUTC: nil,
                                       value: nil, unit: nil, metadata: ["derivation_version": "1"]))
        }
        return try await database.apply(events: derived)
    }

    private func collectWorkoutDetails() async throws -> Int {
        var saved = 0
        var processed = 0
        while !Task.isCancelled, processed < 2_000 {
            let ids = try await database.pendingWorkoutIDs(limit: 50)
            if ids.isEmpty { break }
            for id in ids {
                guard var progress = try await database.workoutProgress(id: id) else { continue }
                if !progress.eventsDone {
                    repeat {
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
                        let page = try await health.collectWorkoutHeartRatePage(workoutID: id, anchorData: anchor)
                        saved += try await database.apply(events: page.events, anchor: page.nextAnchorData, typeIdentifier: anchorKey)
                        anchor = page.nextAnchorData
                        if !page.countReachedLimit { break }
                    } while !Task.isCancelled
                    if !Task.isCancelled { try await database.markWorkoutHeartRateDone(id: id) }
                }
                processed += 1
                if Task.isCancelled { break }
            }
        }
        return saved
    }
}
