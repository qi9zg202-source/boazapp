import SwiftUI
import UIKit

final class BoazAppDelegate: NSObject, UIApplicationDelegate {
    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil) -> Bool {
        Task { @MainActor in await BoazCoordinator.shared.startBackgroundDelivery() }
        return true
    }
}

@main
struct BoazApp: App {
    @UIApplicationDelegateAdaptor(BoazAppDelegate.self) private var appDelegate

    var body: some Scene {
        WindowGroup {
            MainDashboardView(model: BoazCoordinator.shared.model)
        }
    }
}
