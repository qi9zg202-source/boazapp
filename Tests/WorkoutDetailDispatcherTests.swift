import Foundation
import XCTest
@testable import boazapp

final class WorkoutDetailDispatcherTests: XCTestCase {
    private var schemaURL: URL? {
        Bundle(for: WorkoutDetailDispatcherTests.self).url(forResource: "Schema", withExtension: "sql")
    }

    private func database() throws -> (BoazLocalDatabase, URL) {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        let url = directory.appendingPathComponent("health.sqlite")
        return (try BoazLocalDatabase(url: url, schemaURL: schemaURL), url)
    }

    private func workout(_ index: Int, duration: Double = 60) -> HealthEvent {
        let id = String(format: "00000000-0000-0000-0000-%012d", index)
        return HealthEvent(eventID: id, revision: 0, operation: "upsert", kind: "workout", type: "HKWorkoutTypeIdentifier",
                           sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                           startUTC: Date(timeIntervalSince1970: 1_700_000_000), endUTC: Date(timeIntervalSince1970: 1_700_000_060),
                           value: duration, unit: "s", metadata: [:])
    }

    private static func complete(_ id: UUID, in database: BoazLocalDatabase) async -> WorkoutDetailResult {
        do {
            try await database.advanceWorkoutEvents(id: id, nextOffset: 0, done: true)
            try await database.markWorkoutHeartRateDone(id: id)
            return WorkoutDetailResult(savedEvents: 0, failure: nil)
        } catch {
            return WorkoutDetailResult(savedEvents: 0, failure: error.localizedDescription)
        }
    }

    func testUnavailableOldestWorkoutDoesNotStarveOthersAndRetriesAfterRestart() async throws {
        let (ledger, url) = try database()
        let workouts = (0..<4).map { workout($0) }
        _ = try await ledger.apply(events: workouts)
        let failedID = try XCTUnwrap(UUID(uuidString: workouts[0].eventID))
        let now = Date(timeIntervalSince1970: 1_800_000_000)
        let first = await WorkoutDetailDispatcher.run(database: ledger, now: now) { id in
            if id == failedID { return WorkoutDetailResult(savedEvents: 0, failure: "Synthetic workout unavailable") }
            return await Self.complete(id, in: ledger)
        }
        XCTAssertEqual(first.attemptedWorkouts, 4)
        XCTAssertEqual(first.pendingWorkouts, 1)
        XCTAssertEqual(first.deferredWorkouts, 1)
        XCTAssertTrue(first.failedSources.contains { $0.contains(failedID.uuidString) })
        for value in workouts.dropFirst() {
            let progress = try await ledger.workoutProgress(id: UUID(uuidString: value.eventID)!)
            XCTAssertEqual(progress?.eventsDone, true)
            XCTAssertEqual(progress?.heartRateDone, true)
        }
        let audit = try await ledger.audit()
        XCTAssertTrue(audit.contains { $0.outcome == "workout_deferred" && $0.detail.contains(failedID.uuidString) })

        let reopened = try BoazLocalDatabase(url: url, schemaURL: schemaURL)
        let waiting = try await reopened.pendingWorkoutIDs(now: now.addingTimeInterval(299))
        XCTAssertTrue(waiting.isEmpty)
        let eligible = try await reopened.pendingWorkoutIDs(now: now.addingTimeInterval(300))
        XCTAssertEqual(eligible, [failedID])
        let retry = await WorkoutDetailDispatcher.run(database: reopened, now: now.addingTimeInterval(300)) { id in
            await Self.complete(id, in: reopened)
        }
        XCTAssertEqual(retry.attemptedWorkouts, 1)
        XCTAssertEqual(retry.pendingWorkouts, 0)
        XCTAssertEqual(retry.deferredWorkouts, 0)
        XCTAssertTrue(retry.failedSources.isEmpty)
        try await ledger.closeForTesting()
        try await reopened.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testTwoThousandLimitIsPerPassAndRemainingHistoryFinishesNextPass() async throws {
        let (ledger, url) = try database()
        _ = try await ledger.apply(events: (0..<2_001).map { workout($0) })
        let first = await WorkoutDetailDispatcher.run(database: ledger) { id in
            await Self.complete(id, in: ledger)
        }
        XCTAssertEqual(first.attemptedWorkouts, 2_000)
        XCTAssertEqual(first.pendingWorkouts, 1)
        XCTAssertTrue(first.failedSources.contains { $0.contains("1 pending") })
        let second = await WorkoutDetailDispatcher.run(database: ledger) { id in
            await Self.complete(id, in: ledger)
        }
        XCTAssertEqual(second.attemptedWorkouts, 1)
        XCTAssertEqual(second.pendingWorkouts, 0)
        XCTAssertTrue(second.failedSources.isEmpty)
        let final = try await ledger.pendingWorkoutIDs()
        XCTAssertTrue(final.isEmpty)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testPartialChildPageRemainsSavedAndReportedWhenLaterQueryFails() async throws {
        let (ledger, url) = try database()
        let parent = workout(0)
        _ = try await ledger.apply(events: [parent, workout(1)])
        let failedID = try XCTUnwrap(UUID(uuidString: parent.eventID))
        let result = await WorkoutDetailDispatcher.run(database: ledger) { id in
            guard id == failedID else { return await Self.complete(id, in: ledger) }
            do {
                let child = HealthEvent(eventID: "workout:\(id.uuidString):event:0", revision: 0, operation: "upsert", kind: "category",
                                        type: "boaz.workout.event", sourceBundleID: "synthetic.watch", sourceName: "Synthetic Watch",
                                        startUTC: parent.startUTC, endUTC: parent.endUTC, value: 1, unit: "workout-event-type",
                                        metadata: ["workout_id": id.uuidString])
                let saved = try await ledger.apply(events: [child])
                try await ledger.advanceWorkoutEvents(id: id, nextOffset: 1, done: false)
                return WorkoutDetailResult(savedEvents: saved, failure: "Synthetic next-page error")
            } catch {
                return WorkoutDetailResult(savedEvents: 0, failure: error.localizedDescription)
            }
        }
        XCTAssertEqual(result.savedEvents, 1)
        XCTAssertEqual(result.attemptedWorkouts, 2)
        XCTAssertEqual(result.pendingWorkouts, 1)
        let progress = try await ledger.workoutProgress(id: failedID)
        XCTAssertEqual(progress?.offset, 1)
        XCTAssertEqual(progress?.eventsDone, false)
        let child = try await ledger.latestEvent(typeIdentifier: "boaz.workout.event")
        XCTAssertEqual(child?.metadata["workout_id"], failedID.uuidString)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }

    func testWorkoutRevisionResetsRetryAndStartsFreshDetailScan() async throws {
        let (ledger, url) = try database()
        let event = workout(0)
        _ = try await ledger.apply(events: [event])
        let id = try XCTUnwrap(UUID(uuidString: event.eventID))
        let now = Date(timeIntervalSince1970: 1_800_000_000)
        try await ledger.advanceWorkoutEvents(id: id, nextOffset: 100, done: false)
        try await ledger.deferWorkoutDetails(id: id, until: now.addingTimeInterval(300), error: "Synthetic unavailable")
        _ = try await ledger.apply(events: [workout(0, duration: 120)])
        let eligible = try await ledger.pendingWorkoutIDs(now: now)
        let progress = try await ledger.workoutProgress(id: id)
        XCTAssertEqual(eligible, [id])
        XCTAssertEqual(progress?.offset, 0)
        XCTAssertEqual(progress?.eventsDone, false)
        XCTAssertEqual(progress?.heartRateDone, false)
        try await ledger.closeForTesting()
        try FileManager.default.removeItem(at: url.deletingLastPathComponent())
    }
}
