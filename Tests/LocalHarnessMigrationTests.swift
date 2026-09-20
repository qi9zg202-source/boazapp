import Foundation
import XCTest

/// Executes the original 11 core and 3 gateway synthetic scenarios in the
/// Xcode test target, so package-linked production code is exercised directly.
final class LocalHarnessMigrationTests: XCTestCase {
    private func schema() throws -> URL {
        try XCTUnwrap(Bundle.main.url(forResource: "Schema", withExtension: "sql"))
    }

    private func temporaryFolder() throws -> URL {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        return folder
    }

    func testCoreScenarios10000() async throws {
        let folder = try temporaryFolder()
        try await LocalCoreHarness.run(schema: schema(), directory: folder, recordCount: 10_000)
        try FileManager.default.removeItem(at: folder)
    }

    func testCoreScenarios100000() async throws {
        let folder = try temporaryFolder()
        try await LocalCoreHarness.run(schema: schema(), directory: folder, recordCount: 100_000)
        try FileManager.default.removeItem(at: folder)
    }

    func testGatewayScenarios() async throws {
        let folder = try temporaryFolder()
        try await GatewayTransportHarness.run(schema: schema(), directory: folder)
        try FileManager.default.removeItem(at: folder)
    }
}
