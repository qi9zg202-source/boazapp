import Foundation

struct WorkoutDetailResult: Sendable {
    let savedEvents: Int
    let failure: String?
}

struct WorkoutDetailPassReport: Sendable {
    var savedEvents = 0
    var failedSources: [String] = []
    var attemptedWorkouts = 0
    var pendingWorkouts = 0
    var deferredWorkouts = 0
}

/// Isolates unavailable workouts without discarding their checkpoints. The fixed pass
/// clock makes a failed job ineligible for the rest of that pass; the durable deadline
/// makes it eligible again later. A limit bounds one pass, never the retained job list.
enum WorkoutDetailDispatcher {
    static let retryDelay: TimeInterval = 5 * 60

    static func run(database: BoazLocalDatabase, now: Date = Date(), passLimit: Int = 2_000,
                    process: @Sendable (UUID) async -> WorkoutDetailResult) async -> WorkoutDetailPassReport {
        var report = WorkoutDetailPassReport()
        guard (1...2_000).contains(passLimit) else {
            report.failedSources = ["workout detail pass limit"]
            return report
        }
        do {
            while !Task.isCancelled, report.attemptedWorkouts < passLimit {
                let remaining = passLimit - report.attemptedWorkouts
                let ids = try await database.pendingWorkoutIDs(limit: min(50, remaining), now: now)
                if ids.isEmpty { break }
                for id in ids {
                    if Task.isCancelled { break }
                    report.attemptedWorkouts += 1
                    let result = await process(id)
                    report.savedEvents += result.savedEvents
                    if Task.isCancelled { break }
                    if let failure = result.failure {
                        report.failedSources.append("workout details: \(id.uuidString)")
                        try await database.deferWorkoutDetails(id: id, until: now.addingTimeInterval(retryDelay), error: failure)
                    } else {
                        try await database.clearWorkoutRetry(id: id)
                    }
                }
                await Task.yield()
            }
        } catch {
            report.failedSources.append("workout detail queue")
            try? await database.recordFailure(phase: "collect", detail: "workout detail queue: \(error.localizedDescription)")
        }
        do {
            let counts = try await database.workoutQueueCounts(now: now)
            report.pendingWorkouts = counts.pending
            report.deferredWorkouts = counts.deferred
            if counts.pending > 0 {
                report.failedSources.append("workout details: \(counts.pending) pending, \(counts.deferred) deferred")
            }
        } catch {
            report.failedSources.append("workout detail queue status")
        }
        return report
    }
}
